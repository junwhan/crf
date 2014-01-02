package org.hyflow

import scala.concurrent.stm._
import org.hyflow.core.util._
import org.hyflow.api._
import org.hyflow.core.directory.Tracker
import org.eintr.loglady.Logging

class Counter(override val _id: String) extends HObj {
	val value = field(0)
}

class CounterMultiJvmNode1 extends HyflowSuite("Counter test", 4, Array("hyflow.nodes=4")) with Logging {
	def myTest {
		val ctr = new Counter("counter1")
		Tracker.register(ctr)

		for (i <- 1 to 100) {
			log.info("Begin iteration %d", i)
			atomic { implicit txn =>
				val ctr: Counter = Tracker.open("counter1")
				ctr.value() += 1
			}
		}
		barrier("end")
		
		atomic { implicit txn =>
			val ctr: Counter = Tracker.open("counter1")
			assert(ctr.value() == 400)
		}
	}
}

class CounterMultiJvmNode2 extends HyflowSuite("Counter test", 4, Array("id=1")) with Logging {
	def myTest {
		Thread.sleep(300)
		var i = 0

		for (i <- 1 to 100) {
			log.info("Begin iteration %d", i)
			atomic { implicit txn =>
				val ctr: Counter = Tracker.open("counter1")
				ctr.value() += 1
			}
		}
		barrier("end")
	}
}

class CounterMultiJvmNode3 extends HyflowSuite("Counter test", 4, Array("id=2")) with Logging {
	def myTest {
		Thread.sleep(300)
		var i = 0

		for (i <- 1 to 100) {
			log.info("Begin iteration %d", i)
			atomic { implicit txn =>
				val ctr: Counter = Tracker.open("counter1")
				ctr.value() += 1
			}
		}
		barrier("end")
	}
}

class CounterMultiJvmNode4 extends HyflowSuite("Counter test", 4, Array("id=3")) with Logging {
	def myTest {
		Thread.sleep(300)
		var i = 0

		for (i <- 1 to 100) {
			log.info("Begin iteration %d", i)
			atomic { implicit txn =>
				val ctr: Counter = Tracker.open("counter1")
				ctr.value() += 1
			}
		}
		barrier("end")
	}
}