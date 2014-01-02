package org.hyflow

import scala.concurrent.stm._
import org.hyflow.core.util._
import org.hyflow.api._
import org.hyflow.core.directory.Tracker

class OpenAtomicMultiJvmNode1 extends HyflowSuite("OpenAtomic test", 2, Array("hyflow.nodes=2")) {
	def myTest {
		Tracker.register(new Counter("c1"))
		Tracker.register(new Counter("c2"))

		atomic { implicit txn =>
			val c1: Counter = Tracker.open("c1")
			c1.value() += 1
			
			atomic.open { implicit txn =>
				val c2: Counter = Tracker.open("c2")
				c2.value() += 1
			}
		}
	}
}

class OpenAtomicMultiJvmNode2 extends HyflowSuite("OpenAtomic test", 2, Array("id=1")) {
	def myTest {
		Thread.sleep(2000)
		
		atomic { implicit txn =>
			val c1: Counter = Tracker.open("c1")
			c1.value() += 1
			
			atomic.open { implicit txn =>
				val c2: Counter = Tracker.open("c2")
				c2.value() += 1
			}
		}
	}
}

