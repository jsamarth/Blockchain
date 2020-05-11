import threading
import sys
import socket
import time
import socketserver
from ast import literal_eval
import pickle
from hashlib import sha256

# What the messages mean:
# nq = node query. This is to tell other servers to send some nodes to connect to
# ctp = connect to peer. This is to tell a peer that a connection from (host port) 
# 		has been established
# peer = response to nq. Gives a list of peers to connect to 

# Global vars
peers = {}
peers_lock = threading.Lock()

msgs = {}
msgs_lock = threading.Lock()

rcvd_bh_buffer = {}
rcvd_bh_buffer_lock = threading.Lock()

service_socket = {}
service_socket_lock = threading.Lock()

mining_partial_block = None
mining_partial_block_lock = threading.Lock()

mempool_lock = threading.Lock()
pow_lock = threading.Lock()
pow_bh = None
pow_bool = True
pow_verification_needed_bool = False
transction_gossip_bool = False
pow_acquire_bool = False

main_server_lock = threading.Lock()

send_socket = None



def save_object(obj, filename):
    with open(filename, 'wb') as output:  # Overwrites any existing file.
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)



class Transaction:
	def __init__(self, time, txid, withdraw, deposit, amount):
		self.time = time
		self.txid = txid
		self.withdraw = withdraw
		self.deposit = deposit
		self.amount = amount

class partial_block:
    def __init__(self, parenthash, height, miner_ip, miner_port):
        self.parenthash = parenthash
        self.txs = []
        self.height = height
        self.miner_ip= miner_ip
        self.miner_port = miner_port
    def blockhash(self):
        return sha256(pickle.dumps(self)).hexdigest()

class block:
    def __init__(self, nonce, time, partial_block):
    	self.time = time
    	self.nonce = nonce
    	self.partial_block = partial_block

class blockchain:
    def __init__(self, genesis_hash):
        self.blocks = {} # Block:Parent
        self.blockheight = {} # Blockhash: height 
        self.blocks[genesis_hash] = 0
        self.head = genesis_hash # Hash of the head block
        self.longest_chain = [genesis_hash] #List of blockhashes on the longest chain
        self.len = 1
        self.tx_included = {} #TXID: TX at the head
        self.blockdb = {} #Blockhash to block
        self.state_map = {genesis_hash:{}} #Block hash to state mapping
        self.time_processed = {} #Blockhash: time
    def insert(self, block):
        blockhash = block.partial_block.blockhash()
        parenthash = block.partial_block.parenthash
        if parenthash not in self.blocks.keys():
            return "orphan_block"
        height = block.partial_block.height
        # Update Statemap
        temp_state = self.state_map[parenthash].copy()
        (temp_state,validity_bool) = stf(temp_state,block.partial_block.txs)
        if not validity_bool:
            return "invalid_block"
        self.state_map[blockhash] = temp_state
        #Updatestatemap complete
        self.blocks[blockhash] = parenthash
        self.blockheight[blockhash] = height
        self.blockdb[blockhash] = block
        self.time_processed[blockhash] = time.time()
        if self.head == parenthash: # The normal scenario
            self.head = blockhash
            self.len = self.len + 1
            self.longest_chain.append(blockhash)
            #update tx_included
            for tx in block.partial_block.txs:
                self.tx_included[tx.txid] = tx
            #update tx_included complete
        elif height > self.len: #CAUTION: Do not do blockchain.insert for orphan blocks
            connected_ancestor = parenthash
            self.head = blockhash
            self.len = height
            # Now update the longest chain
            new_suffix = [parenthash,blockhash]
            while connected_ancestor not in self.longest_chain:
                connected_ancestor = self.blocks[connected_ancestor] # Look for previous generation
                new_suffix = [connected_ancestor] + new_suffix
            ca_height = self.blockheight[connected_ancestor]
            removed_suffix = self.longest_chain[(ca_height-1):]
            self.longest_chain = self.longest_chain[:(ca_height-1)]
            self.longest_chain = self.longest_chain + new_suffix
            #update tx_included
            for removed_bh in removed_suffix:
                removed_blk = self.blockdb[removed_bh]
                for tx in removed_blk.partial_block.txs:
                    self.tx_included.pop(tx.txid,None)
            for added_bh in new_suffix:
                added_blk = self.blockdb[added_bh]
                for tx in added_blk.partial_block.txs:
                    self.tx_included[tx.txid] = tx
            #update tx_included complete
        else:
            pass
        return "block_processed"
    def head_state(self):
        return self.state_map[self.head]

class orphan_buffer:
    def __init__(self):
        self.blocks_to_parent = {} #Blockhash : parent
        self.blockdb = {} #Blockhash:Block
        self.dependents = {} # Parent: [list of blocks(sons)]
    def check_dependency(self, parenthash):
        if parenthash in self.dependents.keys():
            return self.dependents[parenthash]
        else:
            return []
    def insert(self, block):
        blockhash = block.partial_block.blockhash()
        parenthash = block.partial_block.parenthash
        self.blocks_to_parent[blockhash] = parenthash
        self.blockdb[blockhash] = block
        if parenthash not in self.dependents:
            self.dependents[parenthash] = [blockhash]
        else:
            self.dependents[parenthash].append(blockhash)    

class mempool:
    def __init__(self):
        self.transactions = [] #Need list since we need order
        #self.txid_list = []
    def update_mempool_vanilla(self,block):
        block_txs = block.partial_block.txs
        self.transctions = [tx for tx in self.transactions if tx not in block_txs] #Remove transactions in block
    def update_mempool_state(self,state,tx_included):
        new_state = state.copy() #important to do dict copy
        tx_to_remove = []
        for tx in self.transactions:
        	if tx.txid in tx_included.keys():
        		tx_to_remove.append(tx)
        #print(tx_to_remove)
        for tx in tx_to_remove:
        	self.transactions.remove(tx)
        tx_to_remove=[]
        for tx in self.transactions:
            (new_state, validity_bool) = stf(new_state, [tx])
            if not validity_bool:
                tx_to_remove.append(tx)
        #print(tx_to_remove)
        for tx in tx_to_remove:
        	self.transactions.remove(tx)
    def insert(self,tx): # inserts unvalidated transactions, need to run update_mempool_state before assembling block
        self.transactions.append(tx)
        #self.txid_list.append(tx.txid)
    def get_tx(self,state,tx_included):
        self.update_mempool_state(state,tx_included)
        extracted_tx = self.transactions[:(min(2000,len(self.transactions)))]
        return extracted_tx

def stf(state, txs):
    temp_state = state.copy()
    valid_bool = True
    for tx in txs:
        if tx.withdraw==0:
            if tx.deposit in temp_state.keys():
                temp_state[tx.deposit] = temp_state[tx.deposit] + tx.amount
            else:
                temp_state[tx.deposit] = tx.amount
        else:
        	if tx.withdraw in temp_state.keys():    
	            if temp_state[tx.withdraw] < tx.amount:
	                valid_bool = False
	            else:
	                temp_state[tx.withdraw] = temp_state[tx.withdraw] - tx.amount
	                if tx.deposit in temp_state.keys():
	                    temp_state[tx.deposit] = temp_state[tx.deposit] + tx.amount
	                else:
	                    temp_state[tx.deposit] = tx.amount
        	else:
        	 	valid_bool = False
            
    return (temp_state, valid_bool)   

def process_block(rcvd_blk, rcvd_bh):
    chain_msg = chain.insert(rcvd_blk)
    if chain_msg == "block_processed":
        dependencies = orphan_buffer.check_dependency(rcvd_bh)
        for orp_bh in dependencies:
            orp_blk = orphan_buffer.blockdb[orp_bh]
            process_block(orp_blk,orp_bh)
            orphan_buffer.blocks_to_parent.pop(orp_bh,None)
            orphan_buffer.blockdb.pop(orp_bh,None)
        orphan_buffer.dependents.pop(rcvd_bh,None)
    if chain_msg == "orphan_block":
        orphan_buffer.insert(rcvd_blk)
    print("Chain message: " + chain_msg + " chain head: " + chain.head)    
    return None

def form_partial_block():
    height = chain.len + 1
    parenthash = chain.head
    partial_blk = partial_block(parenthash,height, my_ip, my_port)
    partial_blk.txs = mempool.get_tx(chain.state_map[parenthash], chain.tx_included) #TODO: Not completely correct, bad tx design
    return partial_blk

def block_serialize(block): # Block + blockhash + nonce + parenthash + height + my_ip + my_port + time + [txs]
    line = "BLOCK"
    line = line + " " + str(block.partial_block.blockhash()) + " " + str(block.nonce) 
    line = line + " " + str(block.partial_block.parenthash) + " " + str(block.partial_block.height)
    line = line + " " + str(block.partial_block.miner_ip) + " " + str(block.partial_block.miner_port)
    line = line + " " + str(block.time)
    for tx in block.partial_block.txs:
        line = line + " " + str(tx.time) + " " + str(tx.txid) + " " + str(tx.withdraw)
        line = line + " " + str(tx.deposit) + " " + str(tx.amount)
    return line

def block_deserialize(line): #Pass with BLOCK
    split = line.split(" ")
    len_split = len(split)
    blockhash = split[1]
    nonce = split[2]
    parenthash = split[3]
    height = int(split[4])
    miner_ip = split[5]
    miner_port = int(split[6])
    time = split[7]
    tx_list = []
    num_tx = int(round((len_split - 8)/5))
    for i in range(1,(num_tx+1)):
        tx = Transaction(split[3+5*i+0], split[3+5*i+1], int(split[3+5*i+2]), int(split[3+5*i+3]), int(split[3+5*i+4]))
        tx_list.append(tx)
    partial_blk = partial_block(parenthash,height,miner_ip,miner_port)
    partial_blk.txs = tx_list
    blk = block(nonce, time, partial_blk)
    return blk

def server_socket(host, port):
	class LoggerHandler(socketserver.BaseRequestHandler):
		def handle(self):
			new_peer = None
			id_assigned = False
			p_host, p_port = 0, 0
			file = self.request.makefile('r')
			for line in file:
				#peers_lock.acquire()
				if "ctp" == line.split(' ')[0]:
					_, _, p_host, p_port = line.replace("\n", "").split(' ')
					print(f"new connection from ({p_host}, {p_port})")

					peers_lock.acquire()
					check = (p_host, p_port) not in peers.keys()
					id_assigned = True
					if check:
						new_peer = connect_to_peer(p_host, p_port)
					else:
						new_peer = peers[(p_host, p_port)]
					peers_lock.release()

				if ("nq" == line.split(' ')[0]) and id_assigned:
					msg = "peers " + str(list(peers.keys())[:5])
					new_peer.send(f"{msg}\n".encode())
					bandwidths_file.write(f"{time.time()} {len(msg)}\n")
					bandwidths_file.flush()

				if ("peers" == line.split(' ')[0]) and id_assigned:
					peers_to_add = literal_eval(line.split("=")[1])
					peers_lock.acquire()
					for p in peers_to_add:
						if p not in peers.keys() and not (p[0] == my_ip and p[1] == my_port):
							connect_to_peer(p[0], p[1])
					peers_lock.release()

				if ("TRANSACTION" == line.split(' ')[0]) and id_assigned:

					# print("- in trans ---" + line.replace('\n', '') + "-")
					msg = line.replace('\n', '').split(' ')
					msg_txn_id = msg[2]

					t = Transaction(msg[1], msg_txn_id, int(msg[3]), int(msg[4]), int(msg[5]))

					msgs_lock.acquire()
					check_msgs = msg_txn_id not in msgs.keys()
					if check_msgs:
						msgs[str(msg_txn_id)] = t
					msgs_lock.release()
					if check_msgs:
						txn_rcd_file.write(f"rcvd {time.time()} {msg_txn_id}\n")
						txn_rcd_file.flush()
						mempool_lock.acquire()
						global mempool
						#print(len(mempool.transactions))
						mempool.insert(t)
						mempool_lock.release()
						global transction_gossip_bool
						if transction_gossip_bool:
							peers_lock.acquire()
							peer_dict = peers.copy()
							peers_lock.release()
							basic_broadcast(line, peer_dict)


				if ("BLOCK" == line.split(' ')[0]) and id_assigned:

					# print("- in trans ---" + line.replace('\n', '') + "-")
					rcvd_bh = line.split(' ')[1]
					rcvd_bh_buffer_lock.acquire()
					check_not_rcvd = (rcvd_bh not in rcvd_bh_buffer)
					if check_not_rcvd:
						rcvd_bh_buffer[rcvd_bh] = None
					rcvd_bh_buffer_lock.release()
					if check_not_rcvd:
						rcvd_blk = block_deserialize(line.replace("\n",""))
						pow_validity=True
						global pow_verification_needed_bool
						# Checking PoW
						if pow_verification_needed_bool:
							pow_validity = False
							local_pow_acquire_bool = False
							service_socket_lock.acquire()
							verify_msg = f"VERIFY {str(rcvd_bh)} {str(rcvd_blk.nonce)}\n"
							service_socket["default"].sendall(verify_msg.encode())
							service_socket_lock.release()
							while True:
								time.sleep(0.2)
								pow_lock.acquire()
								global pow_acquire_bool
								global pow_bool
								local_pow_acquire_bool = pow_acquire_bool
								if pow_acquire_bool and pow_bool:
									pow_validity=True
								if pow_acquire_bool:
									pow_acquire_bool=False
								pow_lock.release()
								if local_pow_acquire_bool:
									break
						#TODO: PoW check
						
						if pow_validity:
							process_block(rcvd_blk, rcvd_bh)
							print("Processing block")
							service_socket_lock.acquire()
							mining_partial_block_lock.acquire()
							global mining_partial_block
							mempool_lock.acquire()
							mining_partial_block = form_partial_block()
							mempool_lock.release()
							solve_msg = f"SOLVE {str(mining_partial_block.blockhash())}\n"
							mining_partial_block_lock.release()
							service_socket["default"].sendall(solve_msg.encode())
							service_socket_lock.release()
							
							peers_lock.acquire()
							peer_dict = peers.copy()
							#send_socket.sendall(line.encode())	
							peers_lock.release()
							basic_broadcast(line, peer_dict)
					

					#print("-" + line.replace('\n', '') + "-")
				#peers_lock.release()
				#print("-" + line.replace('\n', '') + "-")

			disconnect_from_peer(p_host, p_port)

	with socketserver.ThreadingTCPServer((host, port), LoggerHandler) as server:
		server.serve_forever()

#NOT USING BROADCAST_SOCKET
def broadcast_socket(host, port):
	class LoggerHandler2(socketserver.BaseRequestHandler):
		def handle(self):
			new_peer = None
			id_assigned = False
			file = self.request.makefile('r')
			for line in file:
				print(line)
				for sock in peers.values():
					sock.send(f"{line}".encode())

			

	with socketserver.ThreadingTCPServer((host, port), LoggerHandler2) as server:
		server.serve_forever()

def basic_broadcast(message, peer_dict):
	if(message.startswith("TRANS")):
		msg = message.replace('\n', '').split(' ')
		msg_txn_id = msg[2]
		txn_rcd_file.write(f"sent {time.time()} {msg_txn_id}\n")
		txn_rcd_file.flush()

	for sock in peer_dict.values():
		try:
			sock.sendall(f"{message}\n".encode())
		except socket.error:
			pass


# Connect to a peer with a host and port, and send it a ctp message 
# CAUTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!: Must be called with peers_lock
def connect_to_peer(host, port):
	try:
		new_peer_sock = socket.create_connection((host, port))
		print(f"connected to ({host}, {port})")
	except:
		print(f"unable to connect to {host}, {port}")
		return

	peers[(host, port)] = new_peer_sock

	# print(f"ctp {my_name} {my_ip} {my_port}\n")
	new_peer_sock.send(f"ctp {my_name} {my_ip} {my_port}\n".encode())
	msg = f"ctp {my_name} {my_ip} {my_port}\n"
	bandwidths_file.write(f"{time.time()} {len(msg)}\n")
	bandwidths_file.flush()

	return new_peer_sock

# Gracefully disconnect from a peer and delete it from the dictionary
def disconnect_from_peer(host, port):
	peers_lock.acquire()
	peers.pop((host, port),None)
	peers_lock.release()
	print(f"disconnected from {host}, {port}")

def service(host, port, service_socket):
	service_info = ('0.0.0.0', 5555)
	service_socket_lock.acquire()
	service_socket["default"] = socket.create_connection(service_info)
	print("Connected to the service")
	service_port=service_socket["default"].getsockname()[1]
	#time.sleep(5)
	msg = f"CONNECT {my_name} {my_ip} {my_port}\n"
	# print(msg)
	service_socket["default"].sendall(msg.encode())
	service_socket_lock.release()

	bandwidths_file.write(f"{time.time()} {len(msg)}\n")
	bandwidths_file.flush()

	while 1:
		service_socket_lock.acquire()
		data = service_socket["default"].recv(1024)
		if not data:
			print("Disconnected from the service")
			break

		data = data.decode()

		if "INTRODUCE" in data:
			introds = data.split('\n')
			for i in range(len(introds)-1):
				new_peer = introds[i].split(' ')[1:]
				peers_lock.acquire()
				check = (new_peer[1],new_peer[2]) not in peers.keys()
				if check:
					connect_to_peer(new_peer[1],new_peer[2])
				peers_lock.release()

		if "DIE" in data:
			print(f"{my_name}, {my_ip} is dying\n")
			peers_lock.acquire()
			for p, sock in peers.items():
				sock.close()
			peers_lock.release()
			_thread.interrupt_main()

		if "TRANSACTION" in data:
			msg = data[data.find("TRANS"):].replace('\n', '')
			msg_txn_id = msg.split(' ')[2]
			msg_split = msg.split(' ')
			t = Transaction(msg_split[1], msg_txn_id, int(msg_split[3]), int(msg_split[4]), int(msg_split[5]))
			#msgs_lock.acquire()
			#msgs[str(my_port) + "-" + str(msg_txn_id)] = t
			#msgs_lock.release()
			peers_lock.acquire()
			peer_dict = peers.copy()
			peers_lock.release()
			#global send_socket
			#send_socket.send(msg.encode())
			basic_broadcast(msg,peer_dict) # Will send transaction to itself using basic_broadcast
			

		if "SOLVED" in data:
			msg = data[data.find("SOLVED"):].replace('\n', '')
			mined_bh = msg.split(' ')[1]
			mined_nonce = msg.split(' ')[2]
			mining_partial_block_lock.acquire()
			global mining_partial_block
			if str(mining_partial_block.blockhash()) == mined_bh:
				current_time = str(time.time())
				mined_block = block(mined_nonce, current_time, mining_partial_block) #BLOCK CREATED
				block_msg = block_serialize(mined_block)
				peers_lock.acquire()
				peer_dict = peers.copy()
				#send_socket.sendall(block_msg.encode())	
				peers_lock.release()
				basic_broadcast(block_msg, peer_dict)
				print("BLOCK MINED with no. of transactions = " + str(len(mining_partial_block.txs)) + "Hash = " + str(mined_block.partial_block.blockhash()))
			mining_partial_block_lock.release() 

		if "VERIFY" in data:
			msg = data[data.find("VERIFY"):].replace('\n', '')
			verify_msg = msg.split(' ')[1]
			verify_bh = msg.split(' ')[2]
			mined_nonce = msg.split(' ')[3]
			pow_lock.acquire()
			global pow_bool
			global pow_acquire_bool
			pow_acquire_bool = True
			if verify_msg == "OK":
				pow_bool = True
			elif verify_msg == "FAIL":
				pow_bool = False
			print("verification done for bh: " + verify_bh)
			pow_lock.release()
		service_socket_lock.release()

if __name__ == "__main__":

	#initializing blockchain managers
	mempool = mempool()
	orphan_buffer = orphan_buffer()
	chain = blockchain(sha256(pickle.dumps(int(0))).hexdigest()) 

	my_name = sys.argv[1]
	my_ip = sys.argv[2]
	my_port = int(sys.argv[3])
	my_broadcast_port = my_port + 8000

	bandwidths_file = open(f"bandwidths/data-{my_name}.txt", "w")
	txn_rcd_file = open(f"transactions/data-{my_name}.txt", "w")
	peers_file = open(f"peers/data-{my_name}.txt", "w")

	#NOT USING BROADCAST SERVER
	broadcast_server_thread = threading.Thread(target=broadcast_socket, args=("127.0.0.1", my_broadcast_port), name="broadcast_thread", daemon=True)
	broadcast_server_thread.start()

	send_info = ('127.0.0.1', my_broadcast_port)
	peers_lock.acquire()
	send_socket = socket.create_connection(send_info)
	print(send_socket)
	peers_lock.release()

	# Start the server thread
	server_thread = threading.Thread(target=server_socket, args=(my_ip, my_port), name="peer_thread", daemon=True)
	server_thread.start()

	# Start the service thread
	service_thread = threading.Thread(target=service, args=(my_ip, my_port, service_socket), name="service_thread", daemon=True)
	service_thread.start()



	peers_lock.acquire()
	connect_to_peer(my_ip, str(my_port)) # connect to self
	peers_lock.release()

	time.sleep(5)

	mining_partial_block_lock.acquire()
	mining_partial_block = form_partial_block()
	solve_msg = f"SOLVE {str(mining_partial_block.blockhash())}\n"
	mining_partial_block_lock.release()
	service_socket_lock.acquire()
	service_socket["default"].sendall(solve_msg.encode())
	service_socket_lock.release()

	time.sleep(5)

	while True:
		# Every 30 seconds, print list of nodes its connected to; 
		# also, probe neighbors for new nodes
		time.sleep(10)
		msg = f"nq"
		peers_lock.acquire()
		peer_dict = peers.copy()
		#send_socket.sendall(msg.encode())
		peers_lock.release()

		peers_file.write(f"{time.time()} peers of ({my_ip}, {my_port}): {str(list(peer_dict.keys()))} \n")
		peers_file.flush()

		basic_broadcast(msg, peer_dict)
		print(chain.longest_chain)
		print(chain.head_state())
		save_object(chain, 'blockchain1.pkl')
		#time.sleep(10)
		#peers_lock.acquire()
		#print("printing peers ....")
		#print(list(peers.keys()))
		#peers_lock.release()

		#msgs_lock.acquire()
		#print("==================" + str(len(msgs.keys())) + "==================")
		#print(list(msgs.keys())[-10:])
		#msgs_lock.release()
