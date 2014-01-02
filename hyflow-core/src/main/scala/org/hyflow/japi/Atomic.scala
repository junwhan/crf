package org.hyflow.japi

import org.hyflow._
import scala.concurrent.stm.InTxn
import scala.concurrent.stm.motstm.OpenNestingBlock

abstract class Atomic[T] {
	def atomically(txn: InTxn): T
	def onCommit(txn: InTxn): Unit = {}
	def onAbort(txn: InTxn): Unit = {}

	def exec(): T = {
		atomic { implicit txn =>
			atomically(txn)
		}
	}
	
	def open(): T = {
		atomic.open { implicit txn =>
			atomically(txn)
		} onCommit { implicit txn =>
			onCommit(txn)
		} onAbort { implicit txn =>
			onAbort(txn)
		}
	}
	
	def config(): T = {
		atomic.config { implicit txn =>
			atomically(txn)
		} onCommit { implicit txn =>
			onCommit(txn)
		} onAbort { implicit txn =>
			onAbort(txn)
		}
	}
}
