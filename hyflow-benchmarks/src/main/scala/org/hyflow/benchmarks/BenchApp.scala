package org.hyflow.benchmarks

import org.hyflow.core.util.HyflowConfig
import org.hyflow.Hyflow
import org.eintr.loglady.Logging
import org.hyflow.api._
import org.hyflow.core.directory.Tracker

object BenchApp extends App with Logging {
	
	val extra = HyflowConfig.init(args)
	
	log.info("Starting Hyflow benchmark.")
	var bench: Benchmark = null
	
	if (HyflowConfig.cfg.get[String]("id") != Some("0"))
		Thread.sleep(1000)

	Hyflow.onInit += onInit
	Hyflow.onReady += onReady
	Hyflow.onShutdown += onShutdown
	
	// Hard-wire MotSTM DTM implementation
	scala.concurrent.stm.impl.STMImpl.select(scala.concurrent.stm.motstm.MotSTM)
	HyflowBackendAccess.configure(scala.concurrent.stm.motstm.HyMot)
	HRef.factory = scala.concurrent.stm.motstm.MotSTM
	
	Hyflow.dir = Tracker
	Hyflow.registerService(Tracker)

	Hyflow.init(args, List(org.hyflow.benchmarks.BenchStatsService))
	Hyflow.system.awaitTermination()
	log.info("Hyflow benchmark completed.")
	
	//
	def onInit() {
		bench = HyflowConfig.cfg[String]("hyflow.workload.benchmark") match {
			case "bank" => new bank.BankBenchmark
			case "counter" => new counter.CounterBenchmark
			case "hashtable" => new hashtable.HashtableBenchmark
			case "skiplist" => new skiplist.SkiplistBenchmark
			case "bst" => new bst.BSTBenchmark
			case "rbt" => new rbt.RBTBenchmark
			case "linkedlist" => new linkedlist.LinkedListBenchmark
			case "tpcc" => new tpcc.TPCCBenchmark
		}
		//bench = new counter.CounterBenchmark()
	}
	
	def onReady() {
		new Thread("bench-master") {
			override def run = bench.runBenchmark()
		}.start()
	}

	def onShutdown() {
		
	}
	
	def arguments = args
}