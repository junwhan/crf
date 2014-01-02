package scala.concurrent.stm.haistm

import org.hyflow.Hyflow
import org.hyflow.core._
import org.hyflow.api._
import org.hyflow.core.util._
import org.hyflow.core.directory.CpTracker
import scala.concurrent.stm._
import scala.concurrent.stm.motstm.MotStats
import scala.concurrent.stm.motstm.TFAClock
//import org.apache.commons.javaflow.Continuation
import sun.misc.Continuation
import org.eintr.loglady.Logging
import scala.collection.mutable
import scala.util.Random
import akka.actor._
import akka.dispatch.{ Await, Future, ExecutionContext, Promise }
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._

/**
 * Thread-local current transaction
 */
private[haistm] object HaiInTxn extends ThreadLocal[HaiInTxn] {
	override def initialValue = new HaiInTxn

	def apply()(implicit mt: MaybeTxn): HaiInTxn = mt match {
		case x: HaiInTxn => x
		case _ => get // create a new one for the current thread
	}

	//TODO: fix this once implemented
	private def active(txn: HaiInTxn): HaiInTxn = if (txn.internalCurrentLevel != null) txn else null
	def dynCurrentOrNull: HaiInTxn = active(get)

	def currentOrNull(implicit mt: MaybeTxn) = active(apply())
}

private[haistm] class HaiInTxn extends HaiInTxnRefOps with skel.AbstractInTxn with Logging {

	import Txn._

	// Current Level
	protected def internalCurrentLevel: HaiTxnLevel = {
		log.debug("Getting current level. | _ = %s", _currentLevel)
		return _currentLevel
	}
	private var _currentLevel: HaiTxnLevel = null
	private var _currentStats: MotStats = null

	// Txn id
	val _hy_txnid = scala.util.Random.nextInt
	def getTxnId: Long = (Hyflow._hy_nodeid << 32L) | this._hy_txnid

	class HaiCkptEntry[Z](exec: TxnExecutor, block: InTxn => Z) extends Runnable {
		def run() {
			log.debug("Starting to run the atomic block.")
			try {
				val res = block(HaiInTxn.this)
				log.debug("Finished running the atomic block.")
				CheckpointStatus.set(CheckpointSuccess(res))
			} catch {
				case e: Exception =>
					log.error(e, "Atomic block ended with exception. | e = %s", e.getMessage())
					CheckpointStatus.set(CheckpointPermFail(e))
				case _ @ e =>
					log.error(e, "Oops, caught something but it's not exception?")
					CheckpointStatus.set(CheckpointPermFail(null))
			}
			// Superfluous save, for consistent handling
			new Continuation save
		}
	}
	
	class HaiCkptResume(ckpt: Continuation) extends Runnable {
		def run() {
			ckpt.resume(null)
		}
	}

	// Interface
	def executor: TxnExecutor = throw new AbstractMethodError
	def status: Status = _currentLevel.statusAsCurrent
	override def rootLevel: HaiTxnLevel = internalCurrentLevel.root
	def currentLevel: NestingLevel = internalCurrentLevel
	def rollback(cause: RollbackCause): Nothing = {
		log.debug("Rolling back txn.")
		val contValue = cause match {
			case c: Txn.OptimisticFailureCause =>
				if (c.category == 'cannot_open_locked_object) {
					// For locked object we only need to go back to the prev checkpoint
					log.debug("Locked object, rolling back one level.")
					_currentLevel.forceRollback(cause)
				} else {
					log.warn("OptimisticRollback category not handled: %s", c.category)
				}
			case _ =>
				_currentLevel.root.chainRollback(Txn.RolledBack(cause))
				log.warn("Rollback cause not handled: %s", cause)
		}
		log.debug("Suspending in rollback. passing up %s", contValue)
		throw CpRollbackError
	}
	def retry(): Nothing = throw new AbstractMethodError
	def retryFor(timeoutNanos: Long) { throw new AbstractMethodError }

	def restartAtInval[Z](exec: TxnExecutor, block: InTxn => Z): Tuple2[HaiTxnLevel, Continuation] = {
		log.debug("Restarting transaction at invalidated checkpoint.")

		// Let's rollback transaction
		var level = _currentLevel.root
		
		if (!HyflowConfig.cfg[Boolean]("hyflow.haistm.emulateFlat")) {
			// Find out how far back we need to abort
			while (level.status == Txn.Active && level.activeChild != null) {
				log.trace("Passed through checkpoint: %s", level)
				level = level.activeChild
			}
			log.trace("Stopped at checkpoint: %s | status = %s | activeChild = %s",
				level, level.status, level.activeChild)
		} else {
			// If we emulate flat nesting, rollback to first ckpoint
			if (level.status == Txn.Active && level.activeChild != null) {
				level = level.activeChild
			}
		}

		// Resume execution at that level
		var cont = level.checkpoint
		level = new HaiTxnLevel(this, exec, level.parLevel, false, MotStats(block), cont)
		if (level.parLevel != null) {
			level.mergeFrom(level.parLevel)
		}
		_currentLevel = level

		if (cont != null) {
			// if we have a checkpoint, resume there
			//cont = Continuation.continueWith(cont)
			cont = Continuation.enter(new HaiCkptResume(cont), null).asInstanceOf[Continuation]
		} else {
			// otherwise, start at beginning
			//cont = Continuation.startWith(new HaiRunCkpt(exec, block))
			cont = Continuation.enter(new HaiCkptEntry(exec, block), null).asInstanceOf[Continuation]
		}

		(level, cont)
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

	protected def startCheckpointedExec[Z](exec: TxnExecutor, block: InTxn => Z): Z = {
		// Set up transaction
		log.debug("Starting checkpointed transaction.",
			this.getClass.getClassLoader)
		var level = new HaiTxnLevel(this, exec, currentLevel.asInstanceOf[HaiTxnLevel], true, MotStats(block), null)
		_currentLevel = level

		log.trace("Setting level. | _currentLevel = %s | this = %s", _currentLevel, this)

		try {
			// Start execution
			var cont = Continuation.enter(new HaiCkptEntry(exec, block), null).asInstanceOf[Continuation]
			log.debug("Returned from first checkpoint.")
			var prevFailures = 0

			while (true) {
				log.debug("Checkpoint iteration. | cont = %s | status = %s",
					cont, CheckpointStatus.get)
				CheckpointStatus.get match {
					case res: CheckpointInterim =>
						log.debug("Checkpoint interim.")
						// remember checkpoint into new level
						level = new HaiTxnLevel(this, exec, level, false, MotStats(block), cont)
						level.mergeFrom(level.parLevel)
						_currentLevel = level
						// continue
						log.debug("Continuing...")
						//cont = Continuation.continueWith(cont)
						cont = Continuation.enter(new HaiCkptResume(cont), null).asInstanceOf[Continuation]
					case res: CheckpointFailure =>
						log.debug("Checkpoint failure.")
						prevFailures += 1
						Await.ready(prepareBackoff(prevFailures), 30 seconds)
						val restartRes = restartAtInval(exec, block)
						level = restartRes._1
						cont = restartRes._2
					case res: CheckpointSuccess =>
						log.debug("Checkpoint success.")
						// This transaction is over, try to commit
						if (tryCommit()) {
							_currentLevel = null
							Hyflow._topCommits += 1
							Hyflow._topAborts += prevFailures
							return res.result.asInstanceOf[Z]
						} else {
							log.debug("Commit failed")
							prevFailures += 1
							Await.ready(prepareBackoff(prevFailures), 30 seconds)
							val restartRes = restartAtInval(exec, block)
							level = restartRes._1
							cont = restartRes._2
						}
					case res: CheckpointPermFail =>
						// This transaction raised an exception. 
						// TODO: Decide to commit or abort
						// Abort and rethrow
						_currentLevel = null
						//TODO: Hyflow._permFails += 1
						Hyflow._topAborts += prevFailures
						throw res.e
					case _@ res =>
						log.error("Continuation returned unexpected object. | obj = %s", res)
						throw new Exception("Wrong return object from Continuation.")
				}
			}
		} catch {
			case e: Throwable =>
				_currentLevel = null
				log.error(e, "ERROR CAUGHT")
		}
		null.asInstanceOf[Z]
	}

	protected def flatCheckpointedExec[Z](exec: TxnExecutor, block: InTxn => Z): Z = {
		//TODO: try/catch  (here or toplevel to allow aborting/committing changes
		log.trace("Entering flat level.")
		val res = block(this)
		log.trace("Leaving flat level.")
		res
	}

	def atomic[Z](exec: TxnExecutor, block: InTxn => Z): Z = {
		if (_currentLevel == null) {
			startCheckpointedExec(exec, block)
		} else {
			flatCheckpointedExec(exec, block)
		}
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

	// TODO: move elsewhere
	def findCachedObject[T <: HObj](id: String): Option[T] = {
		var level = _currentLevel
		val res = level.ocGetCached[T](id)
		log.debug("Trying to find cached object. | id = %s | level = %s | res = %s", id, level, res)
		res
	}
	////

	// TFA. 
	// TODO: How to call this?!?
	def forward(rclk: Long) {
		// TODO: account for stack of txns
		if (rclk > _currentLevel.root.startTime) {
			log.debug("Attempting to forward transaction. | rclk = %s", rclk)

			// Check for read-set validity
			if (rsValidate) {
				// Valid read-set, update Txn start time
				_currentLevel.root.startTime = rclk
			} else {
				// Invalid read-set, abort
				// Since we abort to the last valid level, it is OK to update txn starting time
				_currentLevel.root.startTime = rclk
				CheckpointStatus.set(CheckpointFailure())
				//Continuation.suspend()
				new Continuation save
			}
		}
	}

	//TFA
	private def rsValidate(): Boolean = {
		log.debug("Validating read-set.")

		// Need an ExecutionContext, let's use our ActorSystem's dispatcher
		implicit val executor: ExecutionContext = Hyflow.system.dispatcher

		// Collect a set of all handles to validate
		var crt = _currentLevel
		val txnid = getTxnId
		var handles: Set[Handle[_]] = null
		// TODO: optimize by making a list?
		var fidCache = mutable.Map[HaiTxnLevel, Set[Handle.FID]]()

		while (crt != null) {
			// Make this a set of objects, instead of fields, for the initial implementation (TODO!)
			val crtSet = crt.rsHandleSet
			fidCache(crt) = crtSet.map(_._hy_obj._hy_id)
			if (handles == null) {
				// Only remember handles from last checkpoint
				handles = crtSet
			}
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
			//throw RollbackError
		} else {
			log.debug("Validation succeeded.")
		}
		res
	}

	// TFA: compare newest object version with transaction starting time
	private def checkValidationResp(valRespMsg: StoreProvider.ValidateResp,
		fidCache: Map[HaiTxnLevel, Set[Handle.FID]]): Boolean = {

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

	// TODO
	// TODO
	// TODO
	// TODO: Delay response when lock is unavailable
	// TODO
	// TODO
	// TODO
	// TODO

	private def acquireLocks(): Boolean = {

		val txnid = getTxnId
		log.debug("Trying to acquire locks.")

		// Need an ExecutionContext, let's use our ActorSystem's dispatcher
		implicit val executor: ExecutionContext = Hyflow.system.dispatcher

		// Acquire all locks (in parallel, no need to sort)
		val handles = _currentLevel.wsGetHandles

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
			_currentLevel.root.forceRollback(OptimisticFailureCause('acquire_locks_failed, None))
			false
		} else {
			log.debug("Acquired all locks. | count = %s", resp.size)
			true
		}
	}

	// TODO: reuse this method in finalizeWrites
	private def releaseLocks() {
		// Release all locks (in parallel, no need to sort)
		val root = _currentLevel
		val txnid = getTxnId

		val handles =
			// Handles are retrieved from the write-set items
			// Per-object locks, just to make it easy for the initial implementation (TODO!)
			root.wsGetHandles
		val lockSet = handles.map(_._hy_obj).toSet

		for (o <- lockSet)
			if (o._owner == null)
				log.warn("Trying to release lock with null owner. | obj id = %s", o._id)
			else
				Hyflow.locks.unlock(o, o._hy_id, txnid)
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

	// MotSTM or TFA specific?
	// IMPORTANT TODO: make sure locks get released in case of any kind of failure!!!!
	def tryCommit(): Boolean = {
		log.debug("Attempting open-atomic commit.")
		val root = _currentLevel

		fireBeforeCommitCallbacks()

		// TFA prepare commit goes here

		if (!root.tryActiveToPreparing())
			return false

		if (!acquireLocks())
			return false

		// Re-validate read-set
		if (!rsValidate) {
			log.debug("Validation failed?")
			releaseLocks()
			return false
		}

		fireWhilePreparingCallbacks()

		if (externalDecider != null) {
			// external decider doesn't have to content with cancel by other threads
			if (!root.tryPreparingToPrepared() || !consultExternalDecider()) {
				log.debug("Commit failed #1")
				releaseLocks()
				return false
			}

			root.setCommitting()
		} else {
			// attempt to decide commit
			if (!root.tryPreparingToCommitting()) {
				releaseLocks()
				log.debug("Commit failed #2")
				return false
			}
		}

		//_pendingFailure = fireWhileCommittingCallbacks(_currentLevel.executor)

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

	private def finalizeWrites() {
		log.debug("Finalizing writes.")

		// Deferred register
		for ((id, obj) <- _currentLevel.drGetDeferred) {
			log.debug("Registering new object. | id = %s", id)
			Hyflow.store.put(obj)
			Hyflow.dir.register0(obj, false)
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
								//if (MotCfg.CONDITIONAL_SYNC)
								//	CpTracker.responsiblePeer(obj._id) ! Tracker.UpdateVerMsg(obj._id, obj._hy_ver)
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
								Hyflow.dir.register0(obj, false)
							}
						}
					}
				case None =>
			}
		}
	}
}