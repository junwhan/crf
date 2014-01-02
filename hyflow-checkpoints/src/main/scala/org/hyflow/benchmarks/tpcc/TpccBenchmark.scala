package org.hyflow.benchmarks.tpcc

import org.hyflow.api._
import org.hyflow.benchmarks._
import org.eintr.loglady.Logging
import scala.concurrent.stm._

class TPCCBenchmark extends Benchmark with Logging {
	val name = "cp-tpcc"
		
	def benchInit() {
		val slice = getLocalSlice
		if (slice.contains(0)) {
			// Initialize data
			TpccInit.populateAll()
		}
	}
	
	def benchIter() {
		val opt = rand.nextInt(100)
		if (opt < 4) {
			log.info("Run Transaction: Order Status")
			TpccOps.orderStatus()
		} else if (opt < 8) {
			log.info("Run Transaction: Delivery")
			TpccOps.delivery()
		} else if (opt < 12) {
			log.info("Run Transaction: Stock Level")
			TpccOps.stockLevel()
		} else if (opt < 55) {
			log.info("Run Transaction: Payment")
			TpccOps.payment()
		} else {
			log.info("Run Transaction: New Order")
			TpccOps.newOrder()
		}
	}
	
	def benchCheck() = true
}