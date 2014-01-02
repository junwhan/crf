#!/usr/bin/python

import os
import sys
import subprocess

repeat = int(os.getenv("REPEAT", 1))
test_name = os.getenv("TEST_NAME")
nodes = [int(x) for x in os.getenv("NODES", "4").split()]
bench = os.getenv("BENCH", "bank")

cmd = ["python", "bin/distrib.py"]

for i in range(repeat):
	for n in nodes:
		args = ["-r\"%s -b%s\"" % (" ".join(sys.argv[1:]), bench) ]
		args = args + ["-n%d" % n]
		if test_name != None:
			args = args + ["-I\"%s\"" % test_name]
		print cmd+args 
		subprocess.call(" ".join(cmd + args), shell=True)


