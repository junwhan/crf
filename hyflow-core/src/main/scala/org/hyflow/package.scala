package org

import actors.threadpool.TimeUnit
import org.hyflow.api._

/* scala-stm - (c) 2009-2011, Stanford University, PPL */


// Hyflow2 needs to rewrite this for enhancing the functionality of the executor.
package object hyflow {

	def atomic: org.hyflow.api.HyflowExecutor = org.hyflow.api.HyflowExecutor.defaultAtomic

	/** From ScalaSTM */
	def retry(implicit txn: scala.concurrent.stm.InTxn): Nothing = scala.concurrent.stm.Txn.retry

	/** From ScalaSTM */
	def retryFor(timeout: Long, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit txn: scala.concurrent.stm.InTxn) {
		scala.concurrent.stm.Txn.retryFor(timeout, unit)
	}

	/** From ScalaSTM */
	implicit def wrapChainedAtomic[A](lhs: => A) = new scala.concurrent.stm.PendingAtomicBlock(lhs)
	
	/** Implementing open-nesting handlers. */
	implicit def wrapOpenNested[A](lhs: => A) = new scala.concurrent.stm.motstm.OpenNestingBlock(lhs)
	
}