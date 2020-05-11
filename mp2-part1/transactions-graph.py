import os
import numpy as np
import matplotlib.pyplot as plt
import sys
from collections import OrderedDict

rcvd = {}
sent = {}

exp = "exp1"
if len(sys.argv) == 2:
	exp = sys.argv[1]

for filename in os.listdir('./transactions'):
	with open('./transactions/' + filename, "r") as file:
		for line in file:
			line = line.split(' ')

			if "rcvd" in line:
				if line[2] not in rcvd:
					rcvd[line[2]] = [float(line[1])]
				else:
					rcvd[line[2]].append(float(line[1]))

			if "sent" in line:
				sent[line[2]] = float(line[1])

rcvd = OrderedDict(sorted(rcvd.items(), key=lambda t: t[0]))
sent = OrderedDict(sorted(sent.items(), key=lambda t: t[0]))

txn_ids = []
min_delays = []
max_delays = []
median_delays = []
num_nodes = []

for k, v in rcvd.items():
	txn_ids.append(k)
	min_delays.append((np.min(v) - sent[k]) * 1000)
	max_delays.append((np.max(v) - sent[k]) * 1000)
	median_delays.append((np.median(v) - sent[k]) * 1000)
	num_nodes.append(len(v))

plt.figure()
plt.title('Plot of minimun, maximum and median delays in delivering a message')
plt.ylabel('time in ms')
plt.xlabel('txn id # (relative to time the program started)')
plt.plot(range(len(txn_ids)), min_delays, '.', c='r', label="min")
plt.plot(range(len(txn_ids)), max_delays, '.', c='g', label="max")
plt.plot(range(len(txn_ids)), median_delays, '.', c='b', label="median")
plt.legend(loc="upper right")
plt.savefig(f"transactions-delay-{exp}" + ".png")

plt.figure()
plt.title('Plot of number of nodes the message reached')
plt.ylabel('number of nodes')
plt.xlabel('txn id # (relative to time the program started)')
plt.plot(range(len(txn_ids)), num_nodes, '.', c='k')
plt.savefig(f"transactions-spread-{exp}" + ".png")
