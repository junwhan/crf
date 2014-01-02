#!/usr/bin/env python

import subprocess

MAX_NODES = 48
ITERATIONS = {"flat": 0, "closed": 0, "_check": 2}
BENCH = "tpcc"

for nest in ["flat", "closed", "_check"]:
	for i in range(ITERATIONS[nest]):
		for n in [1, 4, 16]:
			if n <= int(MAX_NODES):
				for ops in [1, ]:
					print("Running on %d nodes, iteration %d/%d, nest=%s, ops=%s" % (n, i, ITERATIONS[nest], nest, ops))
					subprocess.call("sleep 1", shell=True)
					if nest == "_check":
						jarstr = "--jar check"
					else:
						jarstr = "--jar bench -N %s" %  nest
					cmd = "bin/hytest.py -n %d %s -b %s -O %s -w 80 -e 40 --time-out 240 hyflow.haistm.checkpointProb=20" % (n, jarstr, BENCH, ops)
					subprocess.call(cmd, shell=True)


