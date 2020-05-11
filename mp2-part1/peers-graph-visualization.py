import networkx as nx
import matplotlib.pyplot as plt
import time
import os
import numpy as np
import sys
from collections import OrderedDict
from ast import literal_eval

exp = "exp1"
if len(sys.argv) == 2:
	exp = sys.argv[1]

msgs = {}

all_edges = {}

edge_set = set()

for filename in os.listdir('./peers'):
	with open('./peers/' + filename, "r") as file:
		for line in file:
			line = line.replace('\n', '')
			first_node = line[line.find('(') : line.find(':')]
			node_list = literal_eval(line[line.find('[') : ])
			edges = [] 
			for node in node_list:
				edge_set.add((first_node, node))
				edges.append((first_node, node))

			all_edges[line.split(' ')[0]] = edges

all_edges = OrderedDict(sorted(all_edges.items(), key=lambda t: t[0]))

G = nx.Graph()
for pair in edge_set:
	G.add_edge(pair[0], pair[1])

nx.draw(G, with_labels=True)
plt.show()