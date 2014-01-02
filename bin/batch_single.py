#!/usr/bin/env python

import subprocess

MAX_NODES = 10 
ITERATIONS = {"flat": 1, "closed": 0, "_check": 0}
BENCH = "tpcc"

for nest in ["flat"]:
	for i in range(ITERATIONS[nest]):
		for n in [10]:
			if n <= int(MAX_NODES):
				for ops in [1, ]:
					print("Running on %d nodes, iteration %d/%d, nest=%s, ops=%s" % (n, i, ITERATIONS[nest], nest, ops))
					subprocess.call("sleep 1", shell=True)
					if nest == "_check":
						jarstr = "--jar check"
					else:
						jarstr = "--jar bench -N %s" %  nest
					cmd = "bin/hytest.py -n %d %s -b %s -O %s -w 50 -e 40 -r 10 -o 10 --time-out 240 hyflow.haistm.checkpointProb=20" % (n, jarstr, BENCH, ops)
					subprocess.call(cmd, shell=True)


