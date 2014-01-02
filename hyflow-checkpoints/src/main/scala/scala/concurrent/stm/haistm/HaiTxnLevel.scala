package scala.concurrent.stm.haistm

import scala.concurrent.stm._
import scala.concurrent.stm.motstm.AccessHistory;
import scala.concurrent.stm.motstm.MotStats;
import scala.concurrent.stm.motstm.TFAClock;

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import org.eintr.loglady.Logging
//import org.apache.commons.javaflow.Continuation
import sun.misc.Continuation

private[haistm] class HaiTxnLevel(
	val txn: HaiInTxn,
	val executor: TxnExecutor,
	override val parLevel: HaiTxnLevel,
	val openNested: Boolean,
	val stats: MotStats,
	val checkpoint: Continuation) extends skel.AbstractNestingLevel with motstm.AccessHistory with Logging {

	// TFA: transaction start time
	var startTime: Long = TFAClock.get
	
	var valid = true
	
	// Define root
	val root: HaiTxnLevel = if (parLevel == null || openNested) this else parLevel.root
	
	// Holds subsequent checkpoint in execution
	var activeChild: HaiTxnLevel = _
	// Update active child for parent
	if (parLevel != null) {
		parLevel.activeChild = this
	}

	// Variables
	val minRetryTimeoutNanos: Long = 0
	val _state = new AtomicReference[AnyRef]()

	@tailrec final def status: Txn.Status = {
		val raw = _state.get()
		if (raw == null)
			Txn.Active // we encode active as null to make requireActive checks smaller
		else if (raw eq "merged")
			parLevel.status
		else if (raw.isInstanceOf[HaiTxnLevel])
			Txn.Active // child is active
		else
			raw.asInstanceOf[Txn.Status]
	}
	
	def statusAsCurrent: Txn.Status = {
		val raw = _state.get
		if (raw == null)
			Txn.Active
		else
			raw.asInstanceOf[Txn.Status]
	}

	def tryActiveToPreparing(): Boolean = {
		// Race with requestRollback called by other threads
		val f = _state.compareAndSet(null, Txn.Preparing)
		//if (f && txn.commitBarrier != null) 
		//notifyBlockedBarrierMembers()
		f
	}

	/** Must be called from the transaction's thread. */
	def forceRollback(cause: Txn.RollbackCause) {
		val s = rollbackImpl(Txn.RolledBack(cause))
		assert(s.isInstanceOf[Txn.RolledBack])
	}

	// Is this usually called from other threads? TODO: Check
	def requestRollback(cause: Txn.RollbackCause): Txn.Status = {
		if (cause == Txn.ExplicitRetryCause)
			throw new IllegalArgumentException("explicit retry is not available via requestRollback")
		rollbackImpl(Txn.RolledBack(cause))
	}

	// this is tailrec for retries, but not when we forward to child
	private def rollbackImpl(rb: Txn.RolledBack): Txn.Status = {
		val raw = _state.get
		if (raw == null) { // || canAttemptLocalRollback(raw)) {
			// normal case (active transaction, or ...)
			if (_state.compareAndSet(raw, rb)) {
				//notifyCompleted()
				rb
			} else
				// Lost race to update state
				// Need to retry using the new state 
				rollbackImpl(rb)
		} else if (raw eq "merged") {
			// we are now taking our status from our parent
			parLevel.rollbackImpl(rb)
		} else if (raw.isInstanceOf[HaiTxnLevel]) {
			// roll back the child first, then retry
			raw.asInstanceOf[HaiTxnLevel].rollbackImpl(rb)
			rollbackImpl(rb)
		} else {
			// request denied
			val res = raw.asInstanceOf[Txn.Status]
			log.warn("Rollback request denied. | status = %s", res)
			res
		}
		rb
	}
	
	def chainRollback(rb: Txn.RolledBack): Txn.Status = {
		if (activeChild != null)
			activeChild.chainRollback(rb)
		rollbackImpl(rb)
	}
	
	@tailrec
	final def verifyChainOrRollback(cause: Txn.RollbackCause): Boolean = {
		if (!valid) {
			log.debug("In verifyChainOrRollback, invalid level.")
			chainRollback(Txn.RolledBack(cause))
			return false
		}
		log.debug("In verifyChainOrRollback: valid level")
		if (activeChild != null) {
			activeChild.verifyChainOrRollback(cause)
		}
		else
			true
	}
	
	def setCommitting() {
		_state.set(Txn.Committing)
	}

	def setCommitted() {
		_state.set(Txn.Committed)
		//notifyCompleted()
	}

	def tryActiveToCommitted(): Boolean = {
		val f = _state.compareAndSet(null, Txn.Committed)
		//if (f)
		//	notifyCompleted()
		f
	}

	def tryPreparingToPrepared(): Boolean = _state.compareAndSet(Txn.Preparing, Txn.Prepared)

	def tryPreparingToCommitting(): Boolean = _state.compareAndSet(Txn.Preparing, Txn.Committing)

}