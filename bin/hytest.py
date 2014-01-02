#!/usr/bin/env python

import os
import os.path
import argparse
import subprocess
import threading
import time

# Config section here
DEBUG = False
JAVA_CMD = ["-server", "-XX:CompileThreshold=400", 
		"-Xmx512m", "-Xms256m" ]
JAVA_DBG = JAVA_CMD + ["-Xdebug", "-Xrunjdwp:transport=dt_socket,address=8001,server=y"] 

BENCHMARKS = ["bank", "hashtable", "skiplist", "counter", "bst", "rbt", "linkedlist", "tpcc"]
DIRECTORIES = ["tracker"]
LOCK_PROVIDERS = ["Combined_LockStore", "Separated_Lock"]
STORE_PROVIDERS = ["Combined_LockStore", "Separated_Store"]
LOG_LEVELS = ["error", "warn", "info", "debug", "trace"]
PRESETS = ["combstore", "sepstore"]
JAR_FILES = { 
	"bench": "hyflow-benchmarks_2.9.1-0.0.1-one-jar.jar",
	"check" : "hyflow-checkpoints_2.9.1-0.0.1-one-jar.jar"
}
JAR_FILE = JAR_FILES["bench"]
NESTING = ["flat", "closed", "open"]
# End config


# Other constants
HYFLOW_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
JAR_PATHS = [".", HYFLOW_PATH, os.path.join(HYFLOW_PATH, "hyflow-benchmarks", "target", "scala-2.9.1")]


# Return the number of cores
__num_cores = None
def num_cores():
	global __num_cores
	if __num_cores == None:
		f = open("/proc/cpuinfo", "rt")
		for line in f:
			if line.startswith("processor"):
				res = int(line.split(":")[1].strip())
		__num_cores = res + 1
	return __num_cores

# Return the mercurial revision id in the script's folder (follows symlinks)
def hg_rev_id():
	proc = subprocess.Popen(["hg", "id"], cwd=HYFLOW_PATH, stdout=subprocess.PIPE)
	res = proc.communicate()[0]
	return res.split()[0]
	
# Return current hostname
def get_hostname():
	proc = subprocess.Popen(["hostname"], cwd=HYFLOW_PATH, stdout=subprocess.PIPE)
	res = proc.communicate()[0]
	return res.strip()

# Locate hyflow jar file
__jar_file = None
def find_jar():
	global __jar_file
	if __jar_file == None:
		for p in JAR_PATHS:
			fn = os.path.join(p, JAR_FILE)
			if os.path.exists(fn):
				__jar_file = fn
				return fn
	return __jar_file

# Apply common configurations
def apply_presets(args):
	if len(args.presets) == 0:
		return
	presets = args.presets.split(",")
	for preset in presets:
		if preset == "combstore":
			args.lock_provider = "Combined_LockStore"
			args.store_provider = "Combined_LockStore"
		elif preset == "sepstore":
			args.lock_provider = "Separated_Lock"
			args.store_provider = "Separated_Store"
		else:
			print("Warning: Unknown preset %s" % preset)


# Argument parser
def setup_arg_parser():
	parser = argparse.ArgumentParser(description="Runs a Hyflow v2 benchmark.")
	parser.add_argument("raw", type=str, nargs="*", help="Other args to pass directly to the program as configuration overrides (name=value).")
	# Start-up script
	startup = parser.add_argument_group("Start-up script")
	startup.add_argument("-n", "--nodes", type=int, default=1, help="Total number of nodes to participate in this test.")
	startup.add_argument("-s", "--spawn", type=int, default=num_cores(), help="Number of nodes to spawn on this machine.")
	startup.add_argument("-f", "--offset", type=int, default=0, help="Number of nodes previously started on other machines.")
	startup.add_argument("--no-taskset", default=False, action="store_true", help="Disable CPU affinity setting.")
	startup.add_argument("--cores", default=1, type=int, help="Number of cores for each spawned node.")
	startup.add_argument("--no-run", default=False, action="store_true", help="Do not actually run benchmark, just print commands.")
	startup.add_argument("--time-out", type=int, default=200, help="Seconds after which the test is forcefully stopped.")
	startup.add_argument("--presets", type=str, default="", help="Common configuration shorcuts. Choose from: %s" % PRESETS)
	startup.add_argument("--jar", type=str, default="bench", choices=JAR_FILES.keys(), help="Which jar file (test) to run.")
	startup.add_argument("--java", type=str, default="java", help="Which java executable to use.")
	# Hyflow
	hyflow = parser.add_argument_group("Hyflow")
	hyflow .add_argument("--port", type=int, help="Base (coordinator node) port. Additional nodes increment from here.")
	hyflow.add_argument("-c", "--coord", type=str, help='Coordinator node IP address.')
	hyflow.add_argument("-p", "--profiler-wait", type=int, help="Seconds to wait before beginning test (to allow attaching profiler)")
	# MotSTM config
	mot = parser.add_argument_group("MotSTM")
	mot.add_argument("--upd-notify-all", type=bool, help="Use object update notifications for all transactions.")
	mot.add_argument("--default-closed-nesting", type=bool, help="Use closed nesting by default for all sub-transactions.")
	# Modules
	hymods = parser.add_argument_group("Hyflow modules")
	hymods.add_argument("--directory", type=str, choices=DIRECTORIES, help="Directory manager component.")
	hymods.add_argument("--lock-provider", type=str, choices=LOCK_PROVIDERS, help="Lock provider.")
	hymods.add_argument("--store-provider", type=str, choices=STORE_PROVIDERS, help="Store provider.")
	# Logging
	logging = parser.add_argument_group("Logging")
	logging.add_argument("-v", "--log-level", type=str, choices=LOG_LEVELS, help="Log level.")
	logging.add_argument("-i", "--rev-id", type=str, default=hg_rev_id(), help="Revision ID.")
	logging.add_argument("-I", "--test-id", type=str, default="hytest", help="Test ID.")
	logging.add_argument("-H", "--hostname", type=str, default=get_hostname(), help="Hostname.")
	#Workload
	workload = parser.add_argument_group("Workload")
	workload.add_argument("-b", "--benchmark", type=str, choices=BENCHMARKS, default=BENCHMARKS[0], help="Benchmark name to run.")
	workload.add_argument("-o", "--objects", type=int, help="(Benchmark param) Number of objects.")
	workload.add_argument("-r", "--read-only", type=int, help="(Benchmark param) Ratio of read-only transactions to total transactions.")
        workload.add_argument("-O", "--ops", type=int, help="(Benchmark param) Number of ops/subtransactions.")
	# Benchmark Behavior
	bench = parser.add_argument_group("Benchmark behavior")
	bench.add_argument("-t", "--threads", type=int, help="Number of threads injecting transactions.")
	bench.add_argument("-w", "--warmup-time", type=int, help="Seconds to warm up (to allow JIT compilation).")
	bench.add_argument("-e", "--test-time", type=int, help="Seconds to perform test.")
	bench.add_argument("-N", "--nesting", type=str, choices=NESTING, help="Nesting model for running sub-transactions.")
	# Done
	return parser

# Timer thread
class Timer(threading.Thread):
	def __init__(self, timeout, callback, args=()):
		threading.Thread.__init__(self)
		self.callback = callback
		self.timeout = timeout
		self.args = args
		self.daemon = True
		self.start()
	def run(self):
		time.sleep(self.timeout)
		self.callback(self.args)

# Terminate benchmar if it freezes
def kill_all(procs):
	print("Benchmark timed out. Killing processes...")
	for proc in procs:
		proc.kill()

# Put args on command line
def make_args(args, node):
	res = [ "id=%d" % node, "hyflow.nodes=%d" % args.nodes ]
	# Hyflow
	if args.coord != None:
		res.append("hyflow.coord=%s" % args.coord)
	if args.port != None:
		res.append("hyflow.basePort=%s" % args.port)
	if args.profiler_wait != None:
		res.append("hyflow.profilerWait=%s" % args.profiler_wait)
	# Hyflow.Modules
	if args.directory != None:
		res.append("hyflow.modules.directory=%s" % args.directory)
	if args.lock_provider != None:
		res.append("hyflow.modules.locks=%s" % args.lock_provider)
	if args.store_provider != None:
		res.append("hyflow.modules.store=%s" % args.store_provider)
	# Hyflow.MotSTM
	if args.upd_notify_all != None:
		res.append("hyflow.motstm.updateNotifyAll=%s" % args.upd_notify_all)
	if args.default_closed_nesting != None:
		res.append("hyflow.motstm.closedNesting=%s" % str(args.default_closed_nesting).lower())
	# Hyflow.Logging
	if args.log_level != None:
		res.append("hyflow.logging.level=%s" % args.log_level)
	if args.rev_id != None:
		res.append("hyflow.logging.revId=%s" % args.rev_id)
	if args.test_id != None:
		res.append("hyflow.logging.testId=%s" % args.test_id)
	if args.hostname != None:
		res.append("hyflow.logging.hostname=%s" % args.hostname)
	# Hyflow.Workload
	if args.benchmark != None:
		res.append("hyflow.workload.benchmark=%s" % args.benchmark)
	if args.objects != None:
		res.append("hyflow.workload.objects=%s" % args.objects)
	if args.read_only != None:
		res.append("hyflow.workload.readOnlyRatio=%s" % args.read_only)
	if args.ops != None:
		res.append("hyflow.workload.ops=%s" % args.ops)
	# Hyflow.Benchmark
	if args.threads != None:
		res.append("hyflow.benchmark.injectorThreads=%s" % args.threads)
	if args.warmup_time != None:
		res.append("hyflow.benchmark.warmupTime=%s" % args.warmup_time)
	if args.test_time != None:
		res.append("hyflow.benchmark.testTime=%s" % args.test_time)
	if args.nesting != None:
		# TODO: fix this, we don't want to configure motstm here
		if args.nesting == "flat":
			res.append("hyflow.motstm.closedNesting=false")
		elif args.nesting == "closed":
			res.append("hyflow.motstm.closedNesting=true")
	# Raw args
	res.extend(args.raw)
	return res

# Main function
def main():
	args = setup_arg_parser().parse_args()
	
	# Sanity check input
	assert(args.spawn > 0)
	assert(args.offset >= 0)
	assert(args.offset < args.nodes)
	
	# Apply common configuration
	apply_presets(args)
	# Determine jar file
	global JAR_FILE
	JAR_FILE = JAR_FILES[args.jar]
	
	# Find nodes to spawn here
	spawn_last = args.offset + args.spawn
	if spawn_last > args.nodes:
		spawn_last = args.nodes
		args.spawn = args.nodes - args.offset
	
	# Check number of nodes to launch
	if args.spawn * args.cores > num_cores():
		print("Warning: More cores required than available.")
		args.no_taskset = True
	
	# Spawn nodes
	procs = []
	crt_core = 0
	for node in range(args.offset, spawn_last):
		print("Spawning node id=%d" % node)
		cmd = [ args.java ]
		if DEBUG and node == 1:
			cmd = cmd + JAVA_DBG
		else:
			cmd = cmd + JAVA_CMD
		cmd = cmd + ["-jar", find_jar() ] + make_args(args, node)
		if not args.no_taskset:
			cmd = ["taskset", "-c", "%d-%d" % (crt_core, crt_core + args.cores - 1)] + cmd
			crt_core += args.cores
		if args.no_run:
			print cmd
			print(" ".join(cmd))
		else:
			procs.append(subprocess.Popen(cmd))
	
	# Wait for nodes to terminate
	try:
		Timer(args.time_out, kill_all, procs)
		for proc in procs:
			proc.wait()
		print("All processes exitted. Bye.")
	except(KeyboardInterrupt):
		print("User interrupted benchmark. Killing processes...")
		for proc in procs:
			proc.kill()
		print("Bye.")

# Entry point
if __name__ == "__main__":
	main()
