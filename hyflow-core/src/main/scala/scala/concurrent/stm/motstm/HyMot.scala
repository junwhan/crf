package scala.concurrent.stm.motstm

import org.hyflow.api._
import scala.concurrent.stm._

// Interface into MotSTM from Hyflow
object HyMot extends HyflowBackendAccess {
	
	def isPessimisticMode(implicit mt: MaybeTxn): Boolean = {
		val txn = MotInTxn.currentOrNull
		if (txn == null)
			false
		else {
			txn.pessimisticMode
		}
	}

	def recordOpen(obj: HObj)(implicit mt: MaybeTxn): Boolean = {
		val txn = MotInTxn.currentOrNull
		if (txn == null)
			false
		else {
			txn.recordOpen(obj)
			true
		}
	}

	def recordRegister(obj: HObj)(implicit mt: MaybeTxn): Boolean = {
		val txn = MotInTxn.currentOrNull
		if (txn == null || txn.status != Txn.Active)
			false
		else {
			txn.recordRegister(obj)
			true
		}
	}

	def recordDelete(id: String)(implicit mt: MaybeTxn): Boolean = {
		val txn = MotInTxn.currentOrNull
		if (txn == null)
			false
		else {
			txn.recordDelete(id)
			true
		}
	}

	// TODO: unbind from MotInTxn!
	def getCrtTxnId(implicit mt: MaybeTxn): Long = {
		val tx = MotInTxn.currentOrNull
		if (tx == null)
			0
		else
			tx.getTxnId
	}
	
	def forward(rclk: Long)(implicit mt: MaybeTxn) {
		val tx = MotInTxn.currentOrNull
		if (tx != null) {
			// TODO: is it safe to forward to latest clock available now,
			// or should we stick to the old procedure of forwarding to the
			// remote clock value that triggered this operation? 
			tx.forward(rclk)
		}
	}
	
	def findCachedObject[T <: HObj](id: String)(implicit mt: MaybeTxn): Option[T] = {
		val tx = MotInTxn.currentOrNull
		if (tx != null) {
			tx.asInstanceOf[MotInTxn].findCachedObject[T](id)
		} else
			None
	}
}