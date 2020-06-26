import threading
import sys
import socket
import time
import socketserver
from ast import literal_eval
import _thread

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

num_peers_limit = 7

class Transaction:
	def __init__(self, txid, time, withdraw, deposit, amount):
		self.time = time
		self.txid = txid
		self.withdraw = withdraw
		self.deposit = deposit
		self.amount = amount

def server_socket(host, port):
	class LoggerHandler(socketserver.BaseRequestHandler):
		def handle(self):
			new_peer = None
			id_assigned = False
			p_host, p_port = 0, 0
			file = self.request.makefile('r')
			for line in file:
				if line.startswith("ctp"):
					_, _, p_host, p_port = line.replace("\n", "").split(' ')
					# print(f"new connection from ({p_host}, {p_port})")

					peers_lock.acquire()
					check = (p_host, p_port) not in peers.keys()
					id_assigned = True
					if check:
						new_peer = connect_to_peer(p_host, p_port)
					else:
						new_peer = peers[(p_host, p_port)]
					peers_lock.release()

				if (line.startswith("nq")) and id_assigned:
					msg = "peers " + str(list(peers.keys()))
					# print(msg)
					new_peer.send(f"{msg}\n".encode())
					bandwidths_file.write(f"{time.time()} {len(msg)}\n")
					bandwidths_file.flush()

				if (line.startswith("peers")) and id_assigned:
					peers_to_add = literal_eval(line[line.find('[') : ])
					peers_lock.acquire()
					for p in peers_to_add:

						# NOTE: LIMITING THE NUMBER OF PEERS THAT CAN BE CONNECTED TO
						if num_peers_limit <= len(peers.keys()):
							break
						if p not in peers.keys() and not (p[0] == my_ip and p[1] == str(my_port)):
							connect_to_peer(p[0], p[1])
					peers_lock.release()

				if line.startswith("TRANSACTION") and id_assigned:

					# print("- in trans ---" + line.replace('\n', '') + "-")
					msg = line.replace('\n', '').split(' ')
					msg_txn_id = msg[2]

					t = Transaction(msg[1], msg_txn_id, msg[3], msg[4], msg[5])

					msgs_lock.acquire()
					if msg_txn_id not in msgs.keys():
						msgs[str(msg_txn_id)] = t
						basic_broadcast(line)

						txn_rcd_file.write(f"rcvd {time.time()} {msg_txn_id}\n")
						txn_rcd_file.flush()
					msgs_lock.release()


				# print("-" + line.replace('\n', '') + "-")

			disconnect_from_peer(p_host, p_port)

	with socketserver.ThreadingTCPServer((host, port), LoggerHandler) as server:
		server.serve_forever()

def basic_broadcast(message):
	# if(message.startswith("nq")):
	# 	print("broadcasting nq .........\n\n\n\n\n")

	if(message.startswith("TRANS")):
		msg = message.replace('\n', '').split(' ')
		msg_txn_id = msg[2]
		txn_rcd_file.write(f"sent {time.time()} {msg_txn_id}\n")
		txn_rcd_file.flush()
	for sock in peers.values():
		sock.send(f"{message}\n".encode())
		bandwidths_file.write(f"{time.time()} {len(message)}\n")
		bandwidths_file.flush()

# Connect to a peer with a host and port, and send it a ctp message 
# CAUTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!: Must be called with peers_lock
def connect_to_peer(host, port):
	try:
		new_peer_sock = socket.create_connection((host, port))
		# print(f"connected to ({host}, {port})")
	except:
		print("unable to connect to peer")
		# print(f"{e}")
		return

	peers[(host, port)] = new_peer_sock

	# print(f"ctp {my_name} {my_ip} {my_port}\n")
	msg = f"ctp {my_name} {my_ip} {my_port}\n"
	new_peer_sock.send(msg.encode())
	bandwidths_file.write(f"{time.time()} {len(msg)}\n")
	bandwidths_file.flush()
	# print(new_peer_sock)
	# new_peer_sock.close()

	return new_peer_sock

# Gracefully disconnect from a peer and delete it from the dictionary
def disconnect_from_peer(host, port):

	print(f"({host}, {port}) is disconnected")
	peers_lock.acquire()
	del peers[(host, port)]
	peers_lock.release()
	# print(f"disconnected from {host}, {port}")

def service(host, port):
	# 172.22.156.179 is the IP address of samarth3@sp20-cs425-g54-01.cs.illinois.edu
	# 192.168.86.57 = IP address of my machine
	service_info = ("172.22.156.179", 5555) 
	try:	
		service_socket = socket.create_connection(service_info)
	except:
		print("Could not connect to the service")
		sys.exit(1)
	print(f"({my_ip}, {my_port}) is connected to the service")
	service_port=service_socket.getsockname()[1]
	time.sleep(5)
	msg = f"CONNECT {my_name} {my_ip} {my_port}\n"
	# print(msg)
	service_socket.sendall(msg.encode())
	bandwidths_file.write(f"{time.time()} {len(msg)}\n")
	bandwidths_file.flush()

	while 1:
		data = service_socket.recv(1024)
		if not data:
			# print("Disconnected from the service")
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

			t = Transaction(msg[1], msg_txn_id, msg[3], msg[4], msg[5])
			msgs_lock.acquire()
			msgs[str(msg_txn_id)] = t
			msgs_lock.release()

			basic_broadcast(msg)

if __name__ == "__main__":

	my_name = sys.argv[1]
	my_ip = sys.argv[2]
	my_port = int(sys.argv[3])

	bandwidths_file = open(f"bandwidths/data-{my_name}.txt", "w")
	txn_rcd_file = open(f"transactions/data-{my_name}.txt", "w")
	peers_file = open(f"peers/data-{my_name}.txt", "w")

	# Start the server thread
	server_thread = threading.Thread(target=server_socket, args=(my_ip, my_port), name="peer_thread", daemon=True)
	server_thread.start()

	# Start the service thread
	service_thread = threading.Thread(target=service, args=(my_ip, my_port), name="service_thread", daemon=True)
	service_thread.start()

	time.sleep(5)
	while True:
		# Every 30 seconds, print list of nodes its connected to; 
		# also, probe neighbors for new nodes
		time.sleep(10)
		msg = "nq"
		peers_lock.acquire()
		basic_broadcast(msg)
		peers_lock.release()

		# time.sleep(10)
		peers_lock.acquire()
		print(f"\n\npeers of ({my_ip}, {my_port}): " + str(list(peers.keys())))
		peers_file.write(f"{time.time()} peers of ({my_ip}, {my_port}): {str(list(peers.keys()))} \n")
		peers_file.flush()
		peers_lock.release()

		# msgs_lock.acquire()
		# print(f"\n({my_ip}, {my_port}) has " + str(len(msgs.keys())) + " messages. The last 10 messages: " + str(list(msgs.keys())[-10:]))
		# msgs_lock.release()
