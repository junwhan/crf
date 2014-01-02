#!/usr/bin/python

import subprocess
import argparse
import time
from datetime import datetime
from threading import Thread

from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet.endpoints import TCP4ServerEndpoint, TCP4ClientEndpoint
from twisted.internet import reactor

# Constants
PROGRAM = ["python", "bin/hytest.py"]
SLAVES_FILE = "slaves.txt"
SIGNAL = "INT"





# Arguments
def setup_arg_parser():
    parser = argparse.ArgumentParser(description="Runs Hyflow v2 distributed tests.")
    cmds = parser.add_mutually_exclusive_group(required=True)
    cmds.add_argument("-s", "--slave", action="store_true", help="Run a slave listening for commands.")
    cmds.add_argument("-k", "--kill", action="store_true", help="Kill running processes on all slaves.")
    cmds.add_argument("-t", "--terminate", action="store_true", help="Kill running processes and terminate all slaves.")
    cmds.add_argument("-r", "--run", type=str, help="Run Hyflow test on slaves, with args.")
    opts = parser.add_argument_group(title="Options.")
    opts.add_argument("-n", "--nodes", type=int, default=2, help="Number of distributed nodes.")
    opts.add_argument("-a", "--args", type=str, default="", help="Arguments to pass to the tests.")
    opts.add_argument("-f", "--no-file", action="store_true", help="Disable updating slave file.")
    opts.add_argument("-I", "--test-id", default="", help="Test ID.")
    return parser

# Utility functions
def run(cmd):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    output = proc.communicate()[0]
    return output

def getIP():
    routes = run("route -n").split("\n")
    iface = None
    for route in routes:
        parts = route.split()
        if parts[0] == "0.0.0.0":
            iface = parts[-1]
            break
    assert iface != None
    data = run("ifconfig %s" % iface)
    ip = None
    for ln in data.split("\n"):
        if "inet addr:" in ln:
            ip = ln.split("inet addr:")[1].split("  ")[0]
            break
    assert ip != None
    return ip

def getFreeMem():
    data = run("free -m").split("\n")
    for ln in data:
        if "-/+ buffers/cache:" in ln:
            return ln.split()[3]

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





# Slave-side
# Protocol
class SlaveProto(LineReceiver):
    def __init__(self):
        self.done = False
    def disconnect(self):
        self.transport.loseConnection()
    def connectionLost(self, reason):
        if self.done:
            reactor.stop()
    def lineReceived(self, line):
        global runner
        cmd = line.split(">")
        if cmd[0] == "run-get-mem":
            self.sendLine("run-mem>%s" % getFreeMem())
        elif cmd[0] == "run-get-cores":
            self.sendLine("run-cores>%d" % num_cores()) 
        elif cmd[0] == "run-cmd":
            # start thread to run process
            runner = ProgramRunner(cmd[1], self)
            runner.start()
        elif cmd[0] == "kill":
            # Kill
            if runner != None and runner.is_alive():
                print "[Slave: Killing process.]"
                run("kill -s%s %d" % (SIGNAL, runner.pid))
            else:
                print "[Slave: Can not kill process: not running.]"
            self.sendLine("done")
        elif cmd[0] == "term":
            # Exit
            if runner != None and runner.is_alive():
                print "[Slave: Killing process...]"
                run("kill -s%s %d" % (SIGNAL, runner.pid))
            print("[Slave: Terminating...]")
            self.done = True
            self.sendLine("done")
            self.disconnect()
        else:
            print("[Slave Error: unknown command received to slave: %s]" % line)

# Factory
class SlaveFactory(Factory):
    def buildProtocol(self, addr):
        return SlaveProto()

# Start function
def start_slave(args):
    # Add self to slave list (assumes networked file system)
    if not args.no_file:
        f = open(SLAVES_FILE, "at")
        f.write(getIP() + "\n")
        f.close()
    # Listen for commands
    reactor.listenTCP(29999, SlaveFactory())


# Program runner
class ProgramRunner(Thread):
    def __init__(self, cmdline, proto):
        Thread.__init__(self)
        self.cmdline = cmdline
        self.proto = proto
        self.pid = None
    def run(self):
        cmd = PROGRAM + [x.replace("___", " ") for x in self.cmdline.split()]
        print "[Slave Running: %s]" % str(cmd)
        p = subprocess.Popen(cmd , shell=False)
        self.pid = p.pid
        p.wait()
        reactor.callFromThread(self.proto.sendLine, "done")
        




# Master-side
# Protocol
class MasterProto(LineReceiver):
    def __init__(self, parent, args):
        self.args = args
        self.parent = parent
    def disconnect(self):
        self.transport.loseConnection()
    def lineReceived(self, line):
        cmd = line.split(">")
        if cmd[0] in ["run-mem", "run-cores"]:
            # split workload and run commands
            aggr.reg(self, cmd[1])
            if len(aggr.slaves) == self.parent.numSlaves:
                self.parent.start_time = datetime.now()
                aggr.run_all()
        elif cmd[0] == "done":
            self.disconnect()
        else:
            print("Error: unknown message received by master: "+line)
    def connectionLost(self, reason):
        self.parent.numSlaves -= 1
        if self.parent.numSlaves == 0:
            if hasattr(self.parent, "start_time"):
                print "Execution took: %s" % (datetime.now() - self.parent.start_time)
            reactor.stop()
# Factory
class MasterFactory(Factory):
    def __init__(self, args, numSlaves):
        self.args = args
        self.numSlaves = numSlaves
    def buildProtocol(self, addr):
        return MasterProto(self, self.args)

# Callback
def master_onConnect(p, args):
    p.args = args
    if args.kill:
        p.sendLine("kill")
    elif args.terminate:
        p.sendLine("term")
    elif args.run:
        # TODO: Change here to determine distribution based on avi mem or cpu cores
        if False:
            p.sendLine("run-get-mem")
        else:
            p.sendLine("run-get-cores")
    else:
        raise Exception("Unknown command")

# Start function
def start_master(args):
    # Read slave list
    f = open(SLAVES_FILE, "rt")
    slaves = [x.strip() for x in f.readlines()]
    f.close()
    aggr.coord2 = slaves[0]

    # Connect to slaves; send commans
    fact = MasterFactory(args, len(slaves))
    for slave in slaves:
        point = TCP4ClientEndpoint(reactor, slave, 29999)
        d = point.connect(fact)
        d.addCallback(master_onConnect, args)


# Slave aggregator
class SlaveAggregator(object):
    def __init__(self, args):
        self.slaves = []
        self.args = args
        self.coord = None
    def reg(self, slave, mem):
        self.slaves.append( (slave, int(mem)) )
    def run_all(self):
        self.slaves = sorted(self.slaves, key=lambda x: x[0].transport.getPeer().host)
        #self.coord = self.slaves[0][0].transport.getPeer().host
        total = sum([x[1] for x in self.slaves])
        # Coarse assignments
        asgn = [ args.nodes * x[1] / total for x in self.slaves ]
        unassigned = args.nodes - sum(asgn)
        # Leftover fracs
        leftover = [ (args.nodes * x[1] * 1.0 / total - y, x, z) for (x, y, z) in zip(self.slaves, asgn, range(len(self.slaves)))]
        # Sort leftovers
        sl = sorted(leftover, key=lambda x: x[0], reverse=True)
        # Pick first as unassigned, add 1 node
        for i in range(unassigned):
            asgn[sl[i][2]] += 1
	# Pick coordinator
	for i in range(len(self.slaves)):
		if asgn[i] != 0 and self.coord == None:
			self.coord = self.slaves[i][0].transport.getPeer().host
			break
        # Send run command
        offset = 0
        for i in range(len(self.slaves)):
            slave, mem = self.slaves[i]
            host = slave.transport.getPeer().host
            nodes = asgn[i]
            print host, "\t", "%d nodes" % nodes
            cmd = "-n%d -s%d -f%d -c%s %s hyflow.hostname=%s -I\"%s\"" % (args.nodes, nodes, offset, self.coord, self.args.run, host, 
                args.test_id.replace(" ", "___")
            )
            slave.sendLine("run-cmd>"+cmd)
            offset += nodes




# Entry point
def main():
    if args.slave:
        start_slave(args)
    else:
        start_master(args)
    reactor.run()
    
    if not args.slave:
        if not args.no_file:
            if args.terminate:
                import os
                os.remove(SLAVES_FILE)

if __name__ == "__main__":
    runner = None
    args = setup_arg_parser().parse_args()
    aggr = SlaveAggregator(args)
    main()

