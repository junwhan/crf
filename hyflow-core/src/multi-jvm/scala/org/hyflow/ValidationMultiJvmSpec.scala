package org.hyflow

import scala.concurrent.stm._
import org.hyflow.core.util._
import org.hyflow.api._
import org.hyflow.core.directory.Tracker

class DequeNode[T](override val _id: String) extends HObj {
	val prev = field(null.asInstanceOf[DequeNode[T]])
	val next = field(null.asInstanceOf[DequeNode[T]])
	val value = field(null.asInstanceOf[T])
}

class ValidationMultiJvmNode1 extends HyflowSuite("Validation test", 2, Array("hyflow.nodes=2")) {
	def myTest {
		val one = new DequeNode[String]("hello")
		Tracker.register(one)

		Thread.sleep(500)

		atomic { implicit txn =>
			val one: DequeNode[String] = Tracker.open("hello")
			println("Old value: " + one.value())
			assert(one.value() === null)
			one.value() = "node0"
			println("New value: " + one.value())
			assert(one.value() === "node0")
			Thread.sleep(500)
		}
	}
}

class ValidationMultiJvmNode2 extends HyflowSuite("Validation test", 2, Array("id=1")) {
	def myTest {
		Thread.sleep(750)
		var i = 0

		atomic { implicit txn =>
			val one: DequeNode[String] = Tracker.open("hello")
			println("Old value: " + one.value())
			if (i == 0)
				assert(one.value() === null)
			else
				assert(one.value() == "node0")
			one.value() = "node1"
			println("New value: " + one.value())
			assert(one.value() == "node1")
			Thread.sleep(500)
			i += 1
		}
	}
}

