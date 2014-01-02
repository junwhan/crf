package org.hyflow

import scala.concurrent.stm._
import org.hyflow.core.util._
import org.hyflow.api._
import org.hyflow.core.directory.Tracker

class CondSyncMultiJvmNode1 extends HyflowSuite("CondSync test", 2, Array("hyflow.nodes=2")) {
	def myTest {
		println("Starting on node1")
		val ctr = new Counter("id")
		Tracker.register(ctr)
		
		var i = 0

		atomic { implicit txn =>
			val ctr: Counter = Tracker.open("id")
			
			// Non-transactional!
			i += 1
			
			if (ctr.value() == 0) {
				println("Counter still zero.")
				assert( i == 1 )
				retry
			} else {
				assert( i == 2 )
				println("Counter is one now, bye!")
			}
		}
		
		assert( i == 2 )
	}
}

class CondSyncMultiJvmNode2 extends HyflowSuite("CondSync test", 2, Array("id=1")) {
	def myTest {
		Thread.sleep(4000)
		atomic { implicit txn =>
			val one: Counter = Tracker.open("id")
			one.value() += 1
			println("Updating counter.")
		}
	}
}

