package org.hyflow

import org.hyflow.japi._
import org.hyflow.core.directory.Tracker
import scala.concurrent.stm.InTxn

class JCtr(val _id: String) extends JHObj {
	val value = jfield(0)
}

class JapiMultiJvmNode1 extends HyflowSuite("Japi test", 2, Array("hyflow.nodes=2")) {
	def myTest {
		Tracker.register(new JCtr("j1"))

		new Atomic[Int] {
			def atomically(txn: InTxn) = {
				val j1: JCtr = Hyflow.dir.open("j1")
				j1.value.set(j1.value.get + 1)
				j1.value.get
			}
		}
	}
}

class JapiMultiJvmNode2 extends HyflowSuite("Japi test", 2, Array("id=1")) {
	def myTest {
		Thread.sleep(500)
		
		new Atomic[Int] {
			def atomically(txn: InTxn) = {
				val j1: JCtr = Hyflow.dir.open("j1")
				j1.value.set(j1.value.get + 1)
				j1.value.get
			}
		}
	}
}

