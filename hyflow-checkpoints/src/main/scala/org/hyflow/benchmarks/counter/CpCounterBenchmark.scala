package org.hyflow.benchmarks.counter

import org.hyflow.api._
import org.hyflow.Hyflow
import org.hyflow.benchmarks._
import org.eintr.loglady.Logging
import org.hyflow.core.util._
import scala.concurrent.stm._
import scala.concurrent.stm.haistm._


class CpCounterBenchmark extends Benchmark with Logging {
	val name = "cp-counter"

	val COUNTER_STEPS = HyflowConfig.cfg[String](
		"hyflow.workload.counter.steps").split(",").toList.map { x =>
			val pool = HyflowConfig.cfg[String]("hyflow.workload.counter._%s.pool" format x)
			val ops = HyflowConfig.cfg[String]("hyflow.workload.counter._%s.ops" format x)
			Map("name" -> x, "pool" -> pool, "ops" -> ops)
		}

	def benchInit() {
		val slice = getLocalSlice
		if (slice.contains(0)) {
			for (step <- COUNTER_STEPS) {
				for (i <- 0 to Integer.parseInt(step("pool")))
					new Counter("ctr_%s_%d".format(step("name"), i))
			}
		}
	}

	def benchIter() {
		def randId(x: Map[String,String]): String = {
			"ctr_%s_%d".format(x("name"), rand.nextInt(Integer.parseInt(x("pool"))))
		}
		val ids = COUNTER_STEPS.map { x => randomObjIdSet(Integer.parseInt(x("ops")), () => randId(x)) }
		if (randomlyReadOnly)
			CounterOps.get(ids)
		else
			CounterOps.inc(ids)
	}

	def benchCheck() = true
}

object CounterOps {

	def inc(ids: List[Array[String]]) {
		atomic { implicit txn =>
			var skipfirst = true
			for (step <- ids) {
				if (skipfirst) {
					skipfirst = false
				}
				else HyHai.manualCheckpoint()
				for (ctrid <- step) {
					val ctr = Hyflow.dir.open[Counter](ctrid)
					ctr.v() = ctr.v() + 1
				}
			}
		}
	}

	def get(ids: List[Array[String]]) {
		atomic { implicit txn =>
			var skipfirst = true
			for (step <- ids) {
				if (skipfirst) {
					skipfirst = false
				} else HyHai.manualCheckpoint()
				for (ctrid <- step) {
					val ctr = Hyflow.dir.open[Counter](ctrid)
					ctr.v()
				}
			}
		}
	}
}

class Counter(val _id: String) extends HObj with Logging {
	val v = field(0)
}