/**
 *  Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */

package org.hyflow

import org.scalatest._
import org.hyflow.core.util._
import akka.util.Duration

abstract class HyflowSuite(val test_name: String, val nodes: Int, val args: Array[String]) extends FunSuite with BeforeAndAfter {

	var thrd_exception: Throwable = null
	
	before {
		MultiJvmSync.start(getClass.getName, nodes)
	}

	after {
		//system.shutdown()
		//try Await.ready(system.asInstanceOf[ActorSystemImpl].terminationFuture, 5 seconds) catch {
		//	case _: TimeoutException ⇒ system.log.warning("Failed to stop [{}] within 5 seconds", system.name)
		//}
		MultiJvmSync.end(getClass.getName, nodes)
	}
	
	class TestThread extends Thread {
		override def run() {
			try {
				myTest()
				Hyflow.done()
			} catch {
				case e =>
					thrd_exception = e
					//e.printStackTrace()
					Hyflow.shutdown()
			}
		}
	}
	val thrd = new TestThread
	
	def onInit(): Unit = {}
	def onReady(): Unit = thrd.start()
	def onShutdown(): Unit = {}
	
	def myTest(): Unit
		
	test(test_name) {
		val extra = HyflowConfig.init(args)
		if(HyflowConfig.cfg.get[String]("id") != Some("0"))
			Thread.sleep(1000)
			
		Hyflow.onInit +=  onInit  
		Hyflow.onReady += onReady
		Hyflow.onShutdown += onShutdown
		
		Hyflow.init(args)
		Hyflow.system.awaitTermination()
		
		if (thrd_exception != null)
			throw thrd_exception
	}

	def barrier(name: String, timeout: Duration = FileBasedBarrier.DefaultTimeout) = {
		MultiJvmSync.barrier(name, nodes, getClass.getName, timeout)
	}
}

object MultiJvmSync {
	val TestMarker = "MultiJvm"
	val StartBarrier = "multi-jvm-start"
	val EndBarrier = "multi-jvm-end"

	def start(className: String, count: Int) = barrier(StartBarrier, count, className)

	def end(className: String, count: Int) = barrier(EndBarrier, count, className)

	def barrier(name: String, count: Int, className: String, timeout: Duration = FileBasedBarrier.DefaultTimeout) = {
		val Array(testName, nodeName) = className split TestMarker
		val barrier = new FileBasedBarrier(name, count, testName, nodeName, timeout)
		barrier.await()
	}
}
