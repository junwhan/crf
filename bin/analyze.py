#!/usr/bin/python
from tables import *
import numpy
import os.path
import pylab


class HyTest(IsDescription):
    testtime = StringCol(32)
    bench = StringCol(32)
    machine = StringCol(32)
    throughput = Float64Col()
    commits = Int32Col()
    aborts = Int32Col()
    hyflow_nodes = Int32Col()
    hyflow_logging_revId = StringCol(32)
    hyflow_logging_testId = StringCol(32)
    hyflow_logging_hostname = StringCol(128)
    hyflow_workload_benchmark = StringCol(32)
    hyflow_benchmark_warmupTime = Int16Col()
    hyflow_benchmark_testTime = Int16Col()
    hyflow_benchmark_injectorThreads = Int16Col()
    hyflow_workload_objects = Int16Col()
    hyflow_workload_ops = Int16Col()
    hyflow_profilerWait = Int16Col()
    hyflow_hostname = StringCol(32)
    hyflow_coord = StringCol(32)
    tag = StringCol(128)

keymap = {
    "Benchmark": "bench",
    "Ended at": "testtime",
    "Throughput": None,
    "Commits": None,
    "Aborts": None
}

class HyTests(object):
    def __init__(self, h5name = "tests.h5", reset=False):
        if os.path.exists(h5name) and not reset:
            self.h5 = openFile(h5name, mode="a")
            self.group = self.h5.root.main5
            self.table = self.group.tests
        else:
            self.h5 = openFile(h5name, mode="w", title="Hyflow2 tests file")
            self.group = self.h5.createGroup("/", 'main5', 'Main Group 5')
            self.table = self.h5.createTable(self.group, "tests", HyTest, "First table")
        self.row = self.table.row
    
            
def gather_results(fname, row):
    f = open(fname, "rt")
    tag = ""
    for ln in f:
        ln = ln.strip()
        if ln == "===":
            row["tag"] = tag
            row.append()
        elif ln == "":
            continue
        elif ln[0] == "#":
            tag = ln.replace("#", "").strip()
        else:
            d = ln.split(": ", 2)
            if d[0] in keymap.keys():
                key = keymap[d[0]]
                if key == None:
                    key = d[0].lower()
                row[key] = d[1]
            elif d[0] == "Arguments" or d[0] == "Argumens" :
                pairs = d[1][5:-1].split(", ")
                for pair in pairs:
                    d2 = pair.split("=", 2)
                    key = d2[0].replace(".", "_")
                    value = d2[1]
                    if key not in ["id"]:
                        row[key] = value
            else:
                print "Unknown result reported: %s" % ln
        pass
    f.close()

def plot_dict(data, line, label):
    xa = data.keys()
    xa = sorted(xa, key = lambda x: int(x))
    ya = [data[x] for x in xa]
    pylab.plot(xa, ya, line, label=label)

def plot_series(filt, line="k-", label=""):
    print filt
    data = {}
    mean = {}
    std = {}
    for it in hy.table.where(filt):
        print ".",
        n = it["hyflow_nodes"]
        if n not in data.keys():
            data[n] = []
        data[n].append( it["throughput"] )
    for n in data.keys():
        data[n] = numpy.array(data[n])
        mean[n] = data[n].mean()
        std[n] = data[n].std()
    plot_dict(mean, line, label)

def main():
    #plot_series('(tag == "netty local")', "b-", "local 0ms")
    #plot_series('(tag == "netty distrib")', "c-", "3pc 0ms")
    
    #plot_series('(tag == "hynetty local")', "k-", "local 1ms")
    #plot_series('(tag == "hynetty distrib")', "r-", "3pc 1ms")
    bench = "counter"
    ax = pylab.subplot(111)
    filt = "True"
    
    for test in ['"ctrsweep-3steps-flat"']:
    #for test in ['"llsweep-flat"', '"llsweep-closed"']:
    #for test in ['"hashsweep-flat"', '"hashsweep-closed"']:
        if test not in ["hytest"]:
            plot_series('(bench == "%s") & (hyflow_logging_testId == "%s") & (%s)' % (bench, test.replace('"', '\\"'), filt), "x-", test)
    
    for test in ['"ctrsweep-3steps-flat-cpman"', '"ctrsweep-3steps-cpman"']:
    #for test in ['"hashsweep-cp5-flat"', '"hashsweep-cp5"', '"hashsweep-cp30-flat"', '"hashsweep-cp30"']:
    #for test in ['"hashsweep-cp30-flat"', '"hashsweep-cp30"', '"hashsweep-cp100-flat"', '"hashsweep-cp100"']:
        if test not in ["hytest"]:
            plot_series('(bench == "cp-%s") & (hyflow_logging_testId == "%s") & (%s)' % (bench, test.replace('"', '\\"'), filt), "x-", test)
    
    pylab.legend(loc="best")
    pylab.grid(True, which="both")
    #pylab.minorticks_on()
    #ax.set_xticklabels(range(2, 10) + [20], minor=True)
    #ax.set_xlim(2, 20)
    #ax.set_ylim(1000, 40000)
    pylab.show()

if __name__ == "__main__":
    hy = HyTests(reset=False)
    #gather_results("result-lost.txt", hy.row)
    #gather_results("result-rosella.txt", hy.row)
    #hy.h5.flush()
    #exit(0)
    
    testNames = set(hy.table.cols.hyflow_logging_testId[:])
    benchmarks = set(hy.table.cols.bench[:])
    print testNames
    print benchmarks
    main()
    hy.h5.close()

