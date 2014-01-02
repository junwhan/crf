package scala.concurrent.stm.haistm

import org.hyflow.api._
import scala.concurrent.stm._
import org.eintr.loglady.Logging

// Interface into MotSTM from Hyflow
object HyHai extends HyflowBackendAccess with Logging {
	
	def isPessimisticMode(implicit mt: MaybeTxn): Boolean = {
		val txn = HaiInTxn.currentOrNull
		if (txn == null)
			false
		else {
			//txn.pessimisticMode
			false
		}
	}

	def recordOpen(obj: HObj)(implicit mt: MaybeTxn): Boolean = {
		val txn = HaiInTxn.currentOrNull
		if (txn == null)
			false
		else {
			txn.recordOpen(obj)
			true
		}
	}

	def recordRegister(obj: HObj)(implicit mt: MaybeTxn): Boolean = {
		val txn = HaiInTxn.currentOrNull
		if (txn == null || txn.status != Txn.Active)
			false
		else {
			txn.recordRegister(obj)
			true
		}
	}

	def recordDelete(id: String)(implicit mt: MaybeTxn): Boolean = {
		val txn = HaiInTxn.currentOrNull
		if (txn == null)
			false
		else {
			txn.recordDelete(id)
			true
		}
	}

	// TODO: unbind from MotInTxn!
	def getCrtTxnId(implicit mt: MaybeTxn): Long = {
		val tx = HaiInTxn.currentOrNull
		if (tx == null)
			0
		else
			tx.getTxnId
	}
	
	def forward(rclk: Long)(implicit mt: MaybeTxn) {
		val tx = HaiInTxn.currentOrNull
		if (tx != null) {
			// TODO: is it safe to forward to latest clock available now,
			// or should we stick to the old procedure of forwarding to the
			// remote clock value that triggered this operation? 
			tx.forward(rclk)
		}
	}
	
	def findCachedObject[T <: HObj](id: String)(implicit mt: MaybeTxn): Option[T] = {
		val tx = HaiInTxn.currentOrNull
		if (tx != null) {
			tx.asInstanceOf[HaiInTxn].findCachedObject[T](id)
		} else
			None
	}
	
	def manualCheckpoint() {
		import sun.misc.Continuation
		import org.hyflow.core._
		
		log.debug("Suspending (manual checkpoint).")
		
		// trigger checkpoint
		val cont = new Continuation()
		CheckpointStatus.set(CheckpointInterim())
		cont.save()
		
		log.debug("Returned from suspension (manual checkpoint).")
	}
}