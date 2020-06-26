import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict
import sys
import os

bandwidth = {}

exp = "exp1"
if len(sys.argv) == 2:
	exp = sys.argv[1]

for filename in os.listdir('./bandwidths'):
	with open('./bandwidths/' + filename, "r") as file:
		for line in file:
			line_list = line.replace('\n', '').split(" ")
			if len(line_list) != 2:
				continue
			time = line_list[0].split('.')[0]

			if time not in bandwidth:
				bandwidth[time] = int(line_list[1])
			else:
				bandwidth[time] += int(line_list[1])

bandwidth = OrderedDict(sorted(bandwidth.items(), key=lambda t: t[0]))

start_second = int(min(bandwidth.keys()))
end_second = int(max(bandwidth.keys()))

plt.figure()
plt.title('Plot of bandwidth every second, given as bytes/second')
plt.xlabel('time in s, from the start to the end')
plt.ylabel('number of bytes consumed')

for i in range(start_second, end_second+1):
	if str(i) in bandwidth:

		plt.plot(i - start_second, bandwidth[str(i)], '.', c='r')

plt.savefig(f"bandwidth-{exp}.png")
