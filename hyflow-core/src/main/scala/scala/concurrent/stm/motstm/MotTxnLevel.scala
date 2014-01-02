package scala.concurrent.stm
package motstm

//import skel.AbstractNestingLevel
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import org.eintr.loglady.Logging
import scala.concurrent.stm.skel.RollbackError

private[motstm] class MotTxnLevel(
	val txn: MotInTxn,
	val executor: TxnExecutor,
	override val parLevel: MotTxnLevel,
	val openNested: Boolean,
	val stats: MotStats) extends skel.AbstractNestingLevel with AccessHistory with Logging {

	// Define root
	val root: MotTxnLevel = if (parLevel == null || openNested) this else parLevel.root

	// TFA: transaction start time
	var startTime: Long = TFAClock.get

	// Variables
	//val minRetryTimeoutNanos: Long = 0
	val _state = new AtomicReference[AnyRef]()
	var valid = true
	var flatNestedDepth = 0

	// Adapted from CCSTM.
	/*@tailrec final def minEnclosingRetryTimeout(accum: Long = Long.MaxValue): Long = {
		val z = math.min(accum, executor.retryTimeoutNanos.getOrElse(Long.MaxValue))
		if (parLevel == null) z else parLevel.minEnclosingRetryTimeout(z)
	}*/

	@tailrec final def status: Txn.Status = {
		val raw = _state.get()
		if (raw == null)
			Txn.Active // we encode active as null to make requireActive checks smaller
		else if (raw eq "merged")
			parLevel.status
		else if (raw.isInstanceOf[MotTxnLevel])
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

	// Status changes
	// TODO: allow other transactions to wait on this to complete, via message passing
	// i.e. implement notifyCompleted() and awaitCompleted()

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

	def tryActiveToPreparing(): Boolean = {
		val f = _state.compareAndSet(null, Txn.Preparing)
		//if (f && txn.commitBarrier != null)
		//	notifyBlockedBarrierMembers()
		f
	}

	def tryPreparingToPrepared(): Boolean = _state.compareAndSet(Txn.Preparing, Txn.Prepared)

	def tryPreparingToCommitting(): Boolean = _state.compareAndSet(Txn.Preparing, Txn.Committing)

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

	def chainRollback(rb: Txn.RolledBack): Txn.Status = {
		val raw = _state.get
		log.debug("%s", raw)
		if (raw.isInstanceOf[MotTxnLevel]) {
			raw.asInstanceOf[MotTxnLevel].chainRollback(rb)
		}
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
		val raw = _state.get()
		if (raw.isInstanceOf[MotTxnLevel]) {
			raw.asInstanceOf[MotTxnLevel].verifyChainOrRollback(cause)
		} else
			true
	}

	// this is tailrec for retries, but not when we forward to child
	private def rollbackImpl(rb: Txn.RolledBack): Txn.Status = {
		val raw = _state.get
		if (raw == null || canAttemptLocalRollback(raw)) {
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
		} else if (raw.isInstanceOf[MotTxnLevel]) {
			// roll back the child first, then retry
			raw.asInstanceOf[MotTxnLevel].rollbackImpl(rb)
			rollbackImpl(rb)
		} else {
			// request denied
			val res = raw.asInstanceOf[Txn.Status]
			log.warn("Rollback denied. | status = %s", res)
			res
		}
	}

	private def canAttemptLocalRollback(raw: AnyRef): Boolean = raw match {
		case Txn.Prepared => MotInTxn.get eq txn // remote cancel is not allowed while preparing
		case s: Txn.Status => !s.decided
		case ch: MotTxnLevel => ch.rolledBackOrMerged
		case _ => false
	}

	private def rolledBackOrMerged = _state.get match {
		case "merged" => true
		case Txn.RolledBack(_) => true
		case _ => false
	}

	def requireActive() {
		if (_state.get != null)
			status match {
				case Txn.RolledBack(_) => throw RollbackError
				case s => throw new IllegalStateException(s.toString)
			}
	}

	def attemptMerge(): Boolean = {
		// First we need to set the current state to forwarding. Regardless of
		// whether or not this fails we still need to unlink the parent.
		val f = (_state.get == null) && _state.compareAndSet(null, "merged")

		// We must use CAS to unlink ourselves from our parent, because we race
		// with remote cancels.
		if (parLevel._state.get eq this)
			parLevel._state.compareAndSet(this, null)

		f
	}
}