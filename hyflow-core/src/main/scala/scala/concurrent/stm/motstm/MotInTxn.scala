package scala.concurrent.stm
package motstm

import skel._
import akka.actor._
import akka.dispatch.{ Await, Future, ExecutionContext, Promise }
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import org.hyflow.Hyflow
import org.hyflow.api._
import org.hyflow.core.directory._
import org.hyflow.core._
import org.hyflow.core.store._
import org.hyflow.core.util.HyflowConfig
import org.eintr.loglady.Logging
import scala.collection.mutable
import scala.annotation.tailrec
import scala.util.Random
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.stm.motstm.MotUtil.MotCfg

/**
 * Thread-local current transaction
 */
private[motstm] object MotInTxn extends ThreadLocal[MotInTxn] {
	override def initialValue = new MotInTxn

	def apply()(implicit mt: MaybeTxn): MotInTxn = mt match {
		case x: MotInTxn => x
		case _ => get // create a new one for the current thread
	}

	//TODO: fix this once implemented
	private def active(txn: MotInTxn): MotInTxn = if (txn.internalCurrentLevel != null) txn else null
	def dynCurrentOrNull: MotInTxn = active(get)

	def currentOrNull(implicit mt: MaybeTxn) = active(apply())
}

object TFAClock extends PayloadHandler with Logging {
	private val _clk = new AtomicLong(0)

	def inc = _clk.incrementAndGet
	def get = _clk.get

	// Hyflow payload handler
	val name = "tfaclock"
	def incoming(msg: Message) {
		// Retrieving remote clock value
		val rclk = msg.payloads.get("tfaclock")
		if (rclk == None)
			log.warn("Message received with missing <tfaclock> payload. | msg = %s", msg)
		val rclkval = rclk.get.asInstanceOf[Long]
		log.trace("Received message. | clk = %s | msg = %s", rclkval, msg)
		// Updating local node clock
		// TODO: is CAS too strong? Do we want a simple set?
		while (true) {
			val lclk = _clk.get
			if (lclk > rclkval)
				return
			if (_clk.compareAndSet(lclk, rclkval))
				return
			log.debug("CAS failed. (do not change TFAClock.incoming :)")
		}
	}

	def outgoing = Some(_clk.get)
}

private[motstm] class MotInTxn extends MotInTxnRefOps with AbstractInTxn with Logging {

	import Txn._
	import MotUtil._

	// pre-transaction state
	private var _alternatives: List[InTxn => Any] = Nil
	private var _commitHandler: InTxn => Any = null
	private var _abortHandler: InTxn => Any = null

	// TODO: can make TxnId implicit (and make a new case class for it)
	val _hy_txnid = scala.util.Random.nextInt
	def getTxnId: Long = (Hyflow._hy_nodeid << 32L) | this._hy_txnid

	implicit val timeout = Timeout(5 seconds)
	private var _pendingFailure: Throwable = null
	protected var _pessimisticMode: Boolean = MotCfg.ALWAYS_PESSIMISTIC
	def pessimisticMode: Boolean = _pessimisticMode

	// Current Level
	protected def internalCurrentLevel: MotTxnLevel = _currentLevel
	private var _currentLevel: MotTxnLevel = null
	private var _currentStats: MotStats = null

	// Interface
	def executor: TxnExecutor = throw new AbstractMethodError
	def status: Status = _currentLevel.statusAsCurrent // _currentLevel == null ?!?
	override def rootLevel: MotTxnLevel = internalCurrentLevel.root
	def currentLevel: NestingLevel = throw new AbstractMethodError
	def rollback(cause: RollbackCause): Nothing = {
		// We need to grab the version numbers from writes and pessimistic reads
		// before the status is set to rollback, because as soon as the top-level
		// txn is marked rollback other threads can steal ownership.  This is
		// harmless if some other type of rollback occurs.
		//if (isExplicitRetry(cause))
		//	addLatestWritesAsReads(_barging)

		_currentLevel.forceRollback(cause)
		throw RollbackError
	}

	@throws(classOf[InterruptedException])
	def retry(): Nothing = rollback(ExplicitRetryCause(None))

	def retryFor(timeoutNanos: Long) { throw new AbstractMethodError }

	// pre-transaction state
	def pushAlternative(block: InTxn => Any): Boolean = {
		val z = _alternatives.isEmpty
		_alternatives ::= block
		z
	}

	private def takeAlternatives(): List[InTxn => Any] = {
		val z = _alternatives
		_alternatives = Nil
		z
	}

	def setCommitHandler(block: InTxn => Any): Boolean = {
		val z = _commitHandler == null && _abortHandler == null
		_commitHandler = block
		z
	}

	def setAbortHandler(block: InTxn => Any): Boolean = {
		val z = _commitHandler == null && _abortHandler == null
		_abortHandler = block
		z
	}

	private def takeHandlers(): Tuple2[InTxn => Any, InTxn => Any] = {
		val z = (_commitHandler, _abortHandler)
		_commitHandler = null
		_abortHandler = null
		z
	}

	// Cache open object
	def recordOpen(obj: HObj) {
		_currentLevel.ocRecOpen(obj)
	}

	def recordRegister(obj: HObj) {
		_currentLevel.drRecRegister(obj)
		_currentLevel.ocRecOpen(obj)
	}

	def recordDelete(id: String) {
		//_currentLevel.ddRecDelete(id)
	}
	////

	// Transaction execution procedures

	def atomic[Z](exec: TxnExecutor, block: InTxn => Z): Z = {
		// Execute appropriate level
		if (_currentLevel == null)
			openAtomic(exec, block)
		else if (MotCfg.CLOSED_NESTING)
			closedNestedAtomic(exec, block)
		else
			flatNestedAtomic(exec, block)
		// or closedNestedAtomic, depending on case, maybe want to aggregate these two
		// into a single nestedAtomic which then calls the appropriate one
		// CCSTM also sometimes starts with flat and falls back to closed when partial
		// rollbacks are required
		// TODO: how does checkpointing integrate here?
	}

	def configAtomic[Z](exec: TxnExecutor, block: InTxn => Z): Z =
		HyflowConfig.cfg[String]("hyflow.motstm.atomicConfig") match {
			case "flat" => flatNestedAtomic(exec, block)
			case "closed" => closedNestedAtomic(exec, block)
			case "open" => openAtomic(exec, block)
			case _ => throw new Exception("Hyflow config error: hyflow.motstm.atomicConfig must be one of flat, closed, open.")
		}

	def openAtomic[Z](exec: TxnExecutor, block: InTxn => Z): Z = {
		//TODO: Retrieve handlers here and store them in appropriate level
		val handlers = takeHandlers()

		//clearAttemptHistory()
		var prevFailures = 0
		var prevRetries = 0
		_pessimisticMode = MotCfg.ALWAYS_PESSIMISTIC
		
		val topLevel = _currentLevel == null
		val topLevelDbg = if (topLevel) "Top Level" else "Open Nested"
		(while (true) {
			log.info("Begin %s block. | prevFailures = %s | block = %s",
				topLevelDbg, prevFailures, block.##.toHexString, topLevel)
			var level = new MotTxnLevel(this, exec, _currentLevel, true, MotStats(block))

			// pre-determined pessimistic mode?
			if (level.stats.isPessimisticRequired)
				_pessimisticMode = true

			try {
				// successful attempt or permanent rollback either returns a Z or
				// throws an exception != RollbackError
				val res = openAttempt(prevFailures, level, block)
				// Successful attempt here, update stats
				if (topLevel) {
					Hyflow._topAborts += prevFailures
					Hyflow._topCommits += 1
				} else {
					Hyflow._openNestedAborts += prevFailures
					Hyflow._openNestedCommits += 1
				}
				
				// Execute commit handlers registered with this level by its children
				log.debug("Executing Children's onCommit handlers")
				for (commitHandler <- level.onrGetOnCommitHandlers) {
					openAtomic(exec, commitHandler)
				}
				
				// Register own's handlers into parent
				if (level.parLevel != null)
					level.parLevel.onrAddHandlers(handlers._1, handlers._2)
				return res
			} catch {
				case RollbackError =>
				// pass through
			}

			// we are only here if it is a transient rollback or an explicit retry

			// Have to release locks regardless of the cause
			releasePessimisticLocks(level)

			val waitingFor = if (isExplicitRetry(level.status)) {
				log.debug("%s block: explicit retry.", topLevelDbg)
				if (MotCfg.CONDITIONAL_SYNC) {
					// Conditional sync: await retry using notifications
					val updNotif = prepareAwaitRetry(level)
					level = null // help the GC while waiting
					Hyflow._topAborts += prevFailures
					prevFailures = 0
					prevRetries += 1
					updNotif
				} else {
					// Backoff, even on 
					val backoff = prepareBackoff(prevRetries)
					Hyflow._topAborts += prevFailures
					prevFailures = 0
					prevRetries += 1
					backoff
				}
			} else {
				log.debug("%s block: abort.", topLevelDbg)
				val backoff = prepareBackoff(prevFailures)
				prevFailures += 1 // transient rollback, retry
				prevRetries = 0
				backoff
			}
			Await.ready(waitingFor, 30 seconds)
		}).asInstanceOf[Nothing]
	}

	private def closedNestedAtomic[Z](exec: TxnExecutor, block: InTxn => Z): Z = {
		// clear handlers
		takeHandlers()

		var prevFailures = 0
		(while (true) {
			log.debug("Begin closed nested atomic block. | nestedPrevFailures = %s | block = %s",
				prevFailures, block.##.toHexString)
			// fail back to parent if it has been rolled back
			_currentLevel.requireActive()

			val level = new MotTxnLevel(this, exec, _currentLevel, false, MotStats(block))
			try {
				val res = closedNestedAttempt(prevFailures, level, block, -1)
				Hyflow._closedNestedAborts += prevFailures
				Hyflow._closedNestedCommits += 1
				return res
			} catch {
				case RollbackError => 
					
			}

			// we are only here if it is a transient rollback or an explicit retry
			val cause = level.status.asInstanceOf[RolledBack].cause

			// Have to release locks regardless of the cause
			releasePessimisticLocks(level)

			if (isExplicitRetry(cause)) {
				// TODO: smart retry: place hooks on all objects. Objects updated
				// from this level will retry this level only. Objects from ancestors
				// will cause the ancestor to abort. Check if livelock is possible,
				// when an old object is reused after it was updated.
				log.debug("Closed nested atomic block: explicit retry.")
				// TODO: Make sure parent gets notified by any updated object,
				// from this level or up
				_currentLevel.forceRollback(cause)
				throw RollbackError
			}

			log.debug("Closed nested atomic block: abort.")
			prevFailures += 1 // transient rollback, retry
			if (_pessimisticMode && prevFailures > 2) {
				log.debug("Aborting transaction chain due to too many sub-tx aborts in pessimistic mode.")
				_currentLevel.root.chainRollback(Txn.RolledBack(
					OptimisticFailureCause('max_nested_attempts_exceeded, None)))
				throw RollbackError
			}
		}).asInstanceOf[Nothing]
	}

	private def flatNestedAtomic[Z](exec: TxnExecutor, block: InTxn => Z): Z = {
		// Clear handlers
		takeHandlers()

		_currentLevel.flatNestedDepth += 1
		log.debug("Begin flat nested atomic block. | depth = %d", _currentLevel.flatNestedDepth)
		try {
			block(this)
		} catch {
			case x if x != RollbackError && !_currentLevel.executor.isControlFlow(x) =>
				// TODO: fix this
				// partial rollback is required, but we can't do it here
				// _flatNestingAllowed = false
				_currentLevel.forceRollback(OptimisticFailureCause('restart_to_enable_partial_rollback, Some(x)))
				throw RollbackError
		} finally {
			log.debug("End flat nested atomic block. | depth = %d", _currentLevel.flatNestedDepth)
			_currentLevel.flatNestedDepth -= 1
		}
	}

	// Attempt executing the block
	private def openAttempt[Z](prevFailures: Int, level: MotTxnLevel, block: InTxn => Z): Z = {
		beginAttempt(level)
		checkPessimistic(prevFailures, block)
		try {
			runBlock(block)
		} finally {
			rethrowFromStatus(openComplete())
		}
	}

	private def closedNestedAttempt[Z](prevFailures: Int, level: MotTxnLevel, block: InTxn => Z, reusedReadThreshold: Int): Z = {
		beginAttempt(level) //, reusedReadThreshold)
		checkPessimistic(prevFailures, block)
		try {
			runBlock(block)
		} finally {
			rethrowFromStatus(closedNestedComplete())
		}
	}

	protected def beginAttempt(level: MotTxnLevel) {
		val par = level.parLevel
		if (par != null)
			if (!par._state.compareAndSet(null, level)) {
				log.warn("beginAttempt: _state CAS failed. | val = %s", par._state.get)
				par._state.set(level)
			}
		_currentLevel = level
	}

	//TODO: this way, locks are tryed to be released, even though the last
	// execution wasnt actually pessimistic. 
	// TODO: delay setting the pessimistic flag until after restart
	private def checkPessimistic(prevFailures: Int, block: InTxn => Any) {
		if (MotCfg.ALLOW_PESSIMISTIC && !_pessimisticMode && prevFailures > 6) {
			_pessimisticMode = true
			_currentLevel.root.stats.countPessimistic()
			
			// restart if this is not an open transaction
			if (!_currentLevel.openNested) {
				//TODO: I think this way of aborting causes inconsistencies
				log.debug("Restarting to enter pessimistic mode.")
				_currentLevel.root.chainRollback(Txn.RolledBack(OptimisticFailureCause('restart_in_pessimistic_mode, None)))
				throw RollbackError
			}
		}
	}

	private def releasePessimisticLocks(level: MotTxnLevel) {
		if (_pessimisticMode) {
			log.debug("Releasing pessimistic locks.")
			val txnid = getTxnId
			val handles = level.ocGetHandles()
			val lockSet = handles.map(_._hy_obj).toSet
			for (obj <- lockSet)
				if (obj._owner == null)
					log.warn("Trying to release lock with null owner. | obj id = %s", obj._id)
				else
					Hyflow.locks.unlock(obj, obj._hy_id, txnid)
		}
	}

	private def runBlock[Z](block: InTxn => Z): Z = {
		try {
			block(this)
		} catch {
			case x if x != RollbackError && !_currentLevel.executor.isControlFlow(x) => {
				_currentLevel.forceRollback(UncaughtExceptionCause(x))
				null.asInstanceOf[Z]
			}
		}
	}

	private def rethrowFromStatus(status: Status) {
		status match {
			case rb: RolledBack => {
				rb.cause match {
					case UncaughtExceptionCause(x) => throw x
					case _: TransientRollbackCause => throw RollbackError
				}
			}
			case _ =>
		}
	}

	private def openComplete(): Status = {
		if (attemptOpenComplete()) {
			finishOpenCommit()
			Committed
		} else {
			val s = this.status
			val c = s.asInstanceOf[RolledBack].cause
			if (isExplicitRetry(c)) {
				log.info("Open-atomic commit failed (explicit retry). | cause = %s", c)
				finishOpenRetry(s, c)
			} else {
				log.info("Open-atomic commit failed (rollback). | cause = %s", c)
				finishOpenRollback(s, c)
			}
			s
		}
	}

	// MotSTM or TFA specific?
	// IMPORTANT TODO: make sure locks get released in case of any kind of failure!!!!
	private def attemptOpenComplete(): Boolean = {
		log.debug("Attempting open-atomic commit. | mode = %s",
			if (_pessimisticMode) "pessimistic" else "optimistic")
		val root = _currentLevel

		fireBeforeCommitCallbacks()

		// TFA prepare commit goes here

		if (!root.tryActiveToPreparing())
			return false

		if (!acquireLocks())
			return false

		// Re-validate read-set
		if (!rsValidate(false)) {
			releaseLocks()
			return false
		}

		fireWhilePreparingCallbacks()

		if (externalDecider != null) {
			// external decider doesn't have to content with cancel by other threads
			if (!root.tryPreparingToPrepared() || !consultExternalDecider()) {
				releaseLocks()
				return false
			}

			root.setCommitting()
		} else {
			// attempt to decide commit
			if (!root.tryPreparingToCommitting()) {
				releaseLocks()
				return false
			}
		}

		_pendingFailure = fireWhileCommittingCallbacks(_currentLevel.executor)

		// Short circuit commit for read-only transactions, does not increment clock
		if (root.wsCount == 0) {
			releaseLocks() // TODO: handle O/N locks somehow better
			root.setCommitted()
			log.info("Open-atomic atomic block (read-only) commit succeeded.")
			return true
		}

		// Incrementing node-local clock is linearization point
		val clk = TFAClock.inc
		commitWrites(clk)
		// TODO: deferred deletions
		// TODO: deferred new objects

		// TODO: send wakeup messages to any transactions that 
		// are waiting on any of the updated fields/objects.

		// TODO: if local, release lock
		// TODO: register as owner to non-local objects (and discard old locks)
		//for ((handle, v) <- items) {
		//
		//}
		finalizeWrites()

		root.setCommitted()

		log.info("Open-atomic atomic block commit succeeded.")

		return true
	}

	private def consultExternalDecider(): Boolean = {
		try {
			if (!externalDecider.shouldCommit(this))
				_currentLevel.forceRollback(OptimisticFailureCause('external_decision, None))
		} catch {
			case x => _currentLevel.forceRollback(UncaughtExceptionCause(x))
		}
		this.status eq Prepared
	}

	// TFA. 
	// TODO: How to call this?!?
	def forward(rclk: Long) {
		// TODO: account for stack of txns
		if (rclk > _currentLevel.root.startTime) {
			log.debug("Attempting to forward transaction. | rclk = %s", rclk)

			// Check for read-set validity
			if (rsValidate(true)) {
				// Valid read-set, update Txn start time
				_currentLevel.root.startTime = rclk
			} else {
				// Invalid read-set, abort
				// TODO: Each level should maintain list of valid objects, or overall valid flag

			}
		}
	}

	private def commitWrites(clk: Long) {
		log.debug("Committing writes. | clk = %s", clk)
		val items = _currentLevel.wsGetValueList
		for ((handle, v) <- items) {
			// TODO: this works for fields, how about HObjs?
			handle._hy_data = v
			// TODO: should we store the version in a central location? (e.g. version table)
			handle._hy_ver = clk
			// Version objects instead of fields, for the initial implementation. (TODO!)
			handle._hy_obj._hy_ver = clk
		}
	}

	// TODO: reuse this method in finalizeWrites
	private def releaseLocks() {
		// Release all locks (in parallel, no need to sort)
		val root = _currentLevel
		val txnid = getTxnId

		val handles = if (_pessimisticMode) {
			// Handles are retrieved from the open-cache
			root.ocGetHandles
		} else {
			// Handles are retrieved from the write-set items
			// Per-object locks, just to make it easy for the initial implementation (TODO!)
			root.wsGetHandles
		}
		val lockSet = handles.map(_._hy_obj).toSet

		for (o <- lockSet)
			if (o._owner == null)
				log.warn("Trying to release lock with null owner. | obj id = %s", o._id)
			else
				Hyflow.locks.unlock(o, o._hy_id, txnid)
	}

	private def finalizeWrites() {
		log.debug("Finalizing writes.")

		// Deferred register
		for ((id, obj) <- _currentLevel.drGetDeferred) {
			log.debug("Registering new object. | id = %s", id)
			Hyflow.store.put(obj)
			Tracker.register0(obj, false)
		}

		//TODO: deferred deletions

		val items = _currentLevel.wsGetValueList
		var updated = mutable.Set[HObj]()
		val txnid = getTxnId

		// TODO: what if an object is passed to this txn? It won't be in cache
		// Unlock / register / update items in write-set
		for ((handle, v) <- items) {
			_currentLevel.ocGetCached[HObj](handle._hy_obj._id) match {
				case Some(obj) =>
					if (obj != None) {
						if (obj.isLocalObject) {
							// Release lock from local object. No need to update ownership.
							log.debug("Unlocking already local object. | id = %s | ver = %s",
								handle._hy_obj._id, handle._hy_obj._hy_ver)
							// Only unlock objects in the initial implementation (TODO!)
							if (!updated.contains(obj)) {
								updated += obj
								Hyflow.locks.unlock(obj, obj._hy_id, txnid)
								// For update notifications, we have to notify tracker
								// TODO: what if we don't do this via the tracker, so no extra messages need to go out?
								if (MotCfg.CONDITIONAL_SYNC)
									Tracker.responsiblePeer(obj._id) ! MessagesTracker.UpdateVerMsg(obj._id, obj._hy_ver)
							}
						} else {
							// Update ownership
							if ((!updated.contains(obj)) && (obj._owner != null)) {
								updated += obj
								// Release old lock 
								// TODO: make sure lock future acquisition fails at remote site
								Hyflow.store.lost(obj, txnid)
								//obj._owner ! ObjectStore.LostObjMsg(obj._id, getTxnId)

								log.debug("Informing tracker I'm object's new owner. | id = %s", obj._id)
								// TODO: who has the responsibility to store the object? Should tracker
								// be tasked with this as currently is?
								Hyflow.store.put(obj)
								Tracker.register0(obj, false)
							}
						}
					}
				case None =>
			}
		}

		// Also release other pessimistic locks
		if (_pessimisticMode) {
			log.debug("Releasing pessimistic locks.")
			val lockSet = _currentLevel.ocGetHandles.map(_._hy_obj).toSet
			for (obj <- lockSet) {
				if (!updated.contains(obj)) {
					updated += obj
					if (obj._owner == null)
						log.warn("Trying to release lock with null owner. | obj id = %s", obj._id)
					else
						Hyflow.locks.unlock(obj, obj._hy_id, txnid)
				}
			}
		}
	}

	//TFA
	private def rsValidate(abort: Boolean): Boolean = {
		if (_pessimisticMode) {
			log.debug("Skip validating readset (pessimistic mode).")
			return true
		}
		log.debug("Validating read-set.")

		// Need an ExecutionContext, let's use our ActorSystem's dispatcher
		implicit val executor: ExecutionContext = Hyflow.system.dispatcher

		// Collect a set of all handles to validate
		var crt = _currentLevel
		val txnid = getTxnId
		val handles = mutable.Set[Handle[_]]()
		// TODO: optimize by making a list?
		var fidCache = mutable.Map[MotTxnLevel, Set[Handle.FID]]()
		while (crt != null) {
			// Make this a set of objects, instead of fields, for the initial implementation (TODO!)
			val crtSet = crt.rsHandleSet
			fidCache(crt) = crtSet.map(_._hy_obj._hy_id)
			handles ++= crtSet
			crt = crt.parLevel
		}

		// Group handles based on the node they're from
		val handle_grps = mutable.Map[ActorRef, mutable.Set[Handle[_]]]()
		for (h <- handles) {
			val grp = handle_grps.getOrElseUpdate(h._hy_obj._owner, mutable.Set())
			// Add objects instead of fields (for the initial implementation) (TODO!)
			grp += h._hy_obj
		}

		// Send all requests
		val resp_f0 = for ((remote, grp) <- handle_grps if remote != null)
			// First convert to a set of FIDs of parent objects, so we only acquire one lock/object
			yield Hyflow.store.validate(remote, grp.toList, txnid)
		//yield ask(remote, new ObjectStore.ValidateMsg().mapTo[ObjectStore.ValidateRespMsg]
		val resp_f = Future.sequence(resp_f0)

		// Block! Await validation results
		val resp = Await.result(resp_f, 5 seconds)

		// Compute validation outcome. 
		// We want to mark all invalid objects (so we know which level to abort to), thus:
		// * do not use resp.exists(...)
		// * wait for all responses to come back
		val fidCacheIm = fidCache.toMap
		val res = resp.map(checkValidationResp(_, fidCacheIm)).foldLeft(true)(_ && _)

		// Rollback transactions if needed
		if (!res) {
			log.debug("Invalid read-set. Rolling back...")
			_currentLevel.root.verifyChainOrRollback(
				OptimisticFailureCause('invalid_readset, None))
			if (abort)
				throw RollbackError
		} else {
			log.debug("Validation succeeded.")
		}
		res
	}

	// TFA: compare newest object version with transaction starting time
	private def checkValidationResp(valRespMsg: StoreProvider.ValidateResp,
		fidCache: Map[MotTxnLevel, Set[Handle.FID]]): Boolean = {
		
		def failed(fid: (String, Int), ver: Option[Long]) {
			// Validation failed
			log.debug("Validating object failed. | fid = %s | rmtver = %s | txstart = %s", fid, ver, _currentLevel.root.startTime)

			// Mark MotTxnLevel containing invalid object
			var crt = _currentLevel
			while (crt != null) {
				log.debug("In loop. | fidCache(crt) = %s | fid = %s", fidCache(crt), fid)
				if (fidCache(crt).contains(fid)) {
					crt.valid = false
				}
				crt = crt.parLevel
			}
		}
		
		// TODO: fix (automatically process future reply payloads)
		TFAClock.incoming(valRespMsg) //TODO: !!!
		// 
		var res = true
		for ((fid, ver) <- valRespMsg.vers) {
			log.trace("Processing validation response for entry. | fid = %s | ver = %s", fid, ver)
			ver match {
				case None =>
					failed(fid, ver)
					res = false
				case Some(ver2) if ver2 > _currentLevel.root.startTime =>
					failed(fid, ver)
					res = false
				case _ =>
					log.trace("Validating object succeeded. | fid = %s | rmtver = %s | txstart = %s", fid, ver, _currentLevel.root.startTime)
			}
		}
		res
	}

	//TFA -- parallel or ordered lock acq?
	// TODO: Acquire locks for deferred-registration objects, to make sure we don't create the
	// same object twice.
	private def acquireLocks(): Boolean = {
		if (_pessimisticMode) {
			log.debug("Skip acquiring locks (already acquired: pessimistic mode).")
			return true
		}

		val txnid = getTxnId
		log.debug("Trying to acquire locks.")

		// Need an ExecutionContext, let's use our ActorSystem's dispatcher
		implicit val executor: ExecutionContext = Hyflow.system.dispatcher

		// Acquire all locks (in parallel, no need to sort)
		val root = _currentLevel
		val handles = root.wsGetHandles

		// Per-object locks, just to make it easy for the initial implementation (TODO!)
		//val keyMap = handles.map(x => (x._hy_id._1, x._hy_obj._owner)).toMap
		val objMap = handles.map(x => (x._hy_obj._id, x._hy_obj)).toMap

		//val resp_f = Future.sequence(for (h <- handles) yield Hyflow.lock(h._hy_id))
		val resp_f = Future.sequence(for ((key, obj) <- objMap if obj._owner != null)
			yield Hyflow.locks.lock(obj, obj._hy_id, txnid))

		// Block! Await lock results
		val resp = Await.result(resp_f, 5 seconds)

		// Check if any of the locks failed
		if (resp.exists(_.result == false)) {
			// Yes, locks failed. Must release all.
			for (msg <- resp)
				if (msg.result == true)
					Hyflow.locks.unlock(objMap(msg.fid._1), msg.fid, getTxnId)
				else
					log.debug("Lock failed. | fid = %s", msg.fid)
			root.forceRollback(OptimisticFailureCause('acquire_locks_failed, None))
			false
		} else {
			log.debug("Acquired all locks. | count = %s", resp.size)
			true
		}
	}

	protected def finishOpenCommit() {
		//resetAccessHistory()
		val handlers = resetCallbacks()
		val exec = _currentLevel.executor
		//detach()
		_currentLevel.stats.countCommit()

		_currentLevel = _currentLevel.parLevel

		val f = _pendingFailure
		_pendingFailure = null
		fireAfterCompletionAndThrow(handlers, exec, Committed, f)
	}

	private def finishOpenRollback(s: Status, c: RollbackCause) {
		//rollbackAccessHistory(_slot, c)
		val handlers = rollbackCallbacks()
		val exec = _currentLevel.executor
		//detach()
		_currentLevel.stats.countAbort()
		_currentLevel = _currentLevel.parLevel

		val f = _pendingFailure
		_pendingFailure = null
		fireAfterCompletionAndThrow(handlers, exec, s, f)
	}

	private def finishOpenRetry(s: Status, c: RollbackCause) {
		//rollbackAccessHistory(_slot, c)
		val handlers = rollbackCallbacks()

		// don't detach, but we do need to give up the current level
		val exec = _currentLevel.executor
		assert(_currentLevel.wsCount == 0)
		_currentLevel = _currentLevel.parLevel

		if (handlers != null)
			_pendingFailure = fireAfterCompletion(handlers, exec, s, _pendingFailure)

		if (_pendingFailure != null) {
			// scuttle the retry
			//takeRetrySet(_slot)
			val f = _pendingFailure
			_pendingFailure = null
			throw f
		}
	}

	private def closedNestedComplete(): Status = {
		val child = _currentLevel
		if (child.attemptMerge()) {
			// child was successfully merged
			child.parLevel.mergeFrom(child)
			_currentLevel.stats.countCommit()
			_currentLevel = child.parLevel
			Committed
		} else {
			val s = this.status

			// callbacks must be last, because they might throw an exception
			//rollbackAccessHistory(_slot, s.asInstanceOf[RolledBack].cause)
			val handlers = rollbackCallbacks()
			_currentLevel.stats.countAbort()
			_currentLevel = child.parLevel
			if (handlers != null)
				fireAfterCompletionAndThrow(handlers, child.executor, s, null)
			s
		}
	}

	// Creates future to await explicit retry using conditional sync 
	private def prepareAwaitRetry(level: MotTxnLevel) = {
		// Object modified notification policy
		implicit val executor = Hyflow.system.dispatcher
		val timeout = 20 seconds //level.minRetryTimeoutNanos
		val waitOn = level.rsHandleSet.map(_._hy_obj)
		//log.trace("waitOn = %s", waitOn)
		val waitFutures = for (obj <- waitOn) yield ask(Tracker.responsiblePeer(obj._id), MessagesTracker.ReqUpdNotifMsg(obj._id, obj._hy_ver))(timeout)
		//log.trace("waitFutures = %s", waitFutures)
		val firstResponse = Future.firstCompletedOf(waitFutures)
		//log.trace("firstResp = %s", firstResponse)
		// End object modified notifications
		firstResponse
	}

	private def prepareBackoff(prevAttempts: Int) = {
		// Random back-off policy
		// TODO: Move this into contention management module
		implicit val executor = Hyflow.system.dispatcher
		val backoff = (20 * Math.pow(2, Math.min(prevAttempts, 5)) * (1 + Random.nextFloat)).toInt
		log.debug("Random exponential back-off. | sleep = %d | prevAttempts = %d", backoff, prevAttempts)
		val promise = Promise[Boolean]()
		Hyflow.system.scheduler.scheduleOnce(backoff milliseconds) {
			promise.complete(Right(true))
		}
		// End random exponential back-off.
		promise
	}

	// TODO: move elsewhere
	def findCachedObject[T <: HObj](id: String): Option[T] = {
		var level = _currentLevel
		do {
			val res = level.ocGetCached[T](id)
			log.debug("Trying to find cached object. | id = %s | level = %s | res = %s", id, level, res)
			if (res != None) return res
			level = level.parLevel
		} while (level != null)
		None
	}

}