import pickle
from hashlib import sha256
import numpy as np

#CLASS DEFINITIONS, can be ignored

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
        self.time_processed = time.time()
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

file = open("blockchain1.pkl", 'rb')
chain = pickle.load(file)

print("Length of longest chain = " + str(len(chain.longest_chain)))
print("Total number of blocks = " + str(len(chain.blockdb.keys())+1))


#BLOCK DELAY STORED IN block_delay_array 
block_delay_dict = {}
block_delay_array = []
for blockhash in chain.blockdb:
    block = chain.blockdb[blockhash]
    creation_time = float(block.time)
    rcvd_time = chain.time_processed[blockhash]
    delay = rcvd_time - creation_time
    block_delay_dict[blockhash] = delay
    block_delay_array.append(delay)
#print(block_delay_dict)
block_delay_array = np.array(block_delay_array)
block_delay_mean = np.mean(block_delay_array)
print("Average block delay =" + str(np.mean(block_delay_array)))


#TRANSACTION DELAY STORED IN transaction_delay_array 
transaction_delay_dict = {}
transaction_delay_array = []
for blockhash in chain.longest_chain:
    if blockhash != '24c04d87a97bdcefc011d2dd182eba1beaedfe91ae22e632ffb73e5ae7086bed':
        block = chain.blockdb[blockhash]
        creation_time = float(block.time)
        for tx in block.partial_block.txs:
            transaction_time = float(tx.time)
            delay = creation_time - transaction_time
            transaction_delay_dict[tx.txid] = delay
            transaction_delay_array.append(delay)
        
transaction_delay_array = np.array(transaction_delay_array)
transaction_delay_mean = np.mean(transaction_delay_array)
print("Average transaction delay =" + str(np.mean(transaction_delay_array)))


# NUMBER OF FORKS STORED IN num_forks
blockparent = chain.blocks
parentblock = {} 
num_forks = 0
fork_childs = []
for block in blockparent:
    parent = blockparent[block]
    if parent not in parentblock.keys():
        parentblock[parent] = [block]
    else:
        parentblock[parent] = parentblock[parent] + [block]
        num_forks += 1
        fork_childs.append(block)
print("Number of forks: " + str(num_forks))


#LONGEST FORK STORED IN longest_fork
fork_len_dict = {} #from basis blocks
def ancestor_in_lc(blockhash,distance):
    parenthash = blockparent[blockhash]
    distance +=1
    if parenthash not in chain.longest_chain:
        distance = ancestor_in_lc(parenthash,distance)
    return(distance)
for blockhash in fork_childs:
    fork_len_dict[blockhash] = ancestor_in_lc(blockhash,0)
longest_fork = max(fork_len_dict.values())
print("Longest fork length: " + str(longest_fork))

print("INFO: variables to plot stored in variables block_delay_array and transaction_delay_array ")


