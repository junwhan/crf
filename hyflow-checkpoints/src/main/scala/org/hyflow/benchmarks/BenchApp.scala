package org.hyflow.benchmarks

import org.hyflow.core.util.HyflowConfig
import org.hyflow.Hyflow
import org.hyflow.api._
import scala.concurrent.stm.haistm
import org.eintr.loglady.Logging
import org.hyflow.core.directory.CpTracker
import scala.collection.mutable._

class BenchThread extends Thread("bench-master") with Logging {
	
	def onInit() {

	}

	def onReady() {
		val bench = HyflowConfig.cfg[String]("hyflow.workload.benchmark") match {
			case "bank" => new bank.CpBankBenchmark
			case "counter" => new counter.CpCounterBenchmark
			case "hashtable" => new hashtable.CpHashtableBenchmark
			case "skiplist" => new skiplist.CpSkiplistBenchmark
			case "bst" => new bst.CpBSTBenchmark
			case "rbt" => new rbt.CpRBTBenchmark
			case "linkedlist" => new linkedlist.CpLinkedListBenchmark
			case "tpcc" => new tpcc.TPCCBenchmark
		}
		BenchApp.benchName = bench.name
		bench.runBenchmark()
	}

	def onShutdown() {

	}

	override def run = {
		HyflowConfig.init(BenchApp.arguments)
				
		log.info("Starting Hyflow benchmark.")

		if (HyflowConfig.cfg.get[String]("id") != Some("0"))
			Thread.sleep(1000)

		Hyflow.onInit += onInit
		Hyflow.onReady += onReady
		Hyflow.onShutdown += onShutdown

		// Hard-wire HaiSTM DTM implementation
		scala.concurrent.stm.impl.STMImpl.select(scala.concurrent.stm.haistm.HaiSTM)
		HyflowBackendAccess.configure(scala.concurrent.stm.haistm.HyHai)
		HRef.factory = scala.concurrent.stm.haistm.HaiSTM

		val cptrk = new CpTracker
		Hyflow.dir = cptrk
		Hyflow.registerService(cptrk)
		Hyflow.init(BenchApp.arguments, List(org.hyflow.benchmarks.BenchStatsService))
		Hyflow.system.awaitTermination()
		log.info("Hyflow benchmark completed.")

	}
}

object BenchApp extends App with Logging {

	val arguments = args
	val argsStr = WrappedArray.make[String](args).toList.toString
	var benchName: String = _

	System.setProperty("logback.configurationFile", new java.io.File("etc/logback.xml").getAbsolutePath)
	for (arg <- args) {
			val res = arg.split("=", 2)
			if (res.length == 2) {
				res(0) match {
					case "id" => System.setProperty("hyflow_nodeid", res(1))
					case "hyflow.logging.hostname" =>System.setProperty("hyflow_hostname", res(1))
					case _ => 
				}
			}
		}
	
	
	val thrd: Thread = new BenchThread()
	thrd.start()
}