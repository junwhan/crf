package org.hyflow.core.store
/*
import org.hyflow.core.util._
import org.hyflow.api._
import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout
import akka.dispatch.Future
import scala.collection.mutable
import org.eintr.loglady.Logging
import org.hyflow.core.directory.Tracker


object Separated_Lock extends Service with LockProvider {
	private implicit val timeout = Timeout(5 seconds)
	
	// classes
	case class LockReqMsg(fid: Handle.FID, txnid: Long) extends Message
	case class LockReqPh2Msg(m: LockReqMsg) extends Message
	case class LockReleaseMsg(fid: Handle.FID, txnid: Long) extends Message
	// this is an internal message
	case class EnsureUnlockedVer(txnid: Long, vers: Map[Handle.FID, Option[Long]])
	case class EnsureUnlockedObj(txnid: Long, obj: HObj, pessimistic: Boolean)

	// hyflow service stuff
	val name = "separated-lock"
	val actorProps = Props[Separated_Lock_Actor]
	def accepts(message: Any): Boolean = message match {
		case _: LockReqMsg => true
		case _: LockReleaseMsg => true
		case _ => false
	}
	
	def lock(obj: HObj, fid: Handle.FID, txnid: Long): Future[LockProvider.LockResp] = {
		ask(obj._owner, LockReqMsg(fid, txnid)).asInstanceOf[Future[LockProvider.LockResp]]
	}
	
	def unlock(obj:HObj, fid: Handle.FID, txnid: Long) {
		obj._owner ! LockReleaseMsg(fid, txnid)
	}
}

// TODO: make re-entrant? what for? O/N
// To make re-entrant, store which transaction acquired the lock, and how many times
// Txn id = Node id (ActorRef) + Txn hash ## + Top level count ??

// TODO: add lock timeout instead of bailing out right away

// TODO: merge LockHolder with ObjHolder
class Separated_Lock_Actor extends Actor with Logging {
	import Separated_Lock._

	val locks = mutable.Map[Handle.FID, Long]()

	def receive() = {
		case m: LockReqMsg =>
			// TODO: optimize object check?
			val oldval = locks.get(m.fid)
			if (oldval == None) {
				Separated_Store.ref.forward(m)
			} else if (oldval.get == m.txnid) {
				log.debug("Remote peer requested lock for local object. | fid = %s | granted = %s", m.fid, true)
				sender ! LockProvider.LockResp(m.fid, true)
			} else { 
				log.debug("Remote peer requested lock for local object. | fid = %s | granted = %s", m.fid, false)
				sender ! LockProvider.LockResp(m.fid, false)
			}
		case m: LockReqPh2Msg =>
			val m2 = m.m
			val oldval = locks.get(m2.fid)
			val res = if (oldval == None) {
				locks.put(m2.fid, m2.txnid)
				true
			} else if (oldval.get == m2.txnid) 
				true
			else 
				false
			log.debug("Remote peer requested lock for local object. | fid = %s | granted = %s | sender = %s", 
					m2.fid, res, sender)
			sender ! LockProvider.LockResp(m2.fid, res)
		case m: LockReleaseMsg =>
			log.debug("Remote peer unlocked local object. fid = %s", m.fid)
			locks.remove(m.fid)
		// This gets received internally
		case m: EnsureUnlockedVer =>
			log.trace("Received forwarded validation. | sender = %s | vers = %s", sender, m.vers)
			val vers2 = for ((key, value) <- m.vers) yield {
				val oldval = locks.get(key)
				if (oldval == None || oldval.get == m.txnid) {
					// Lock is free, or it's txn's own lock.
					log.trace("Lock is free. | key = %s | value = %s", key, value)
					(key, value)
				} else {
					log.debug("Validation failed due to locked object. | key = %s | ver = %s", key, value)
					(key, None)
				}

			}
			sender ! new StoreProvider.ValidateResp(vers2)
		// Internally
		case m: EnsureUnlockedObj =>
			val lockval = locks.get(m.obj._hy_id)
			if (lockval == None) {
				if (m.pessimistic) {
					log.debug("Pessimistic get locking object. | id = %s", m.obj._hy_id)
					locks.put(m.obj._hy_id, m.txnid)
				} else {
					log.trace("OK, replying to sender.")
				}
				sender ! StoreProvider.GetResp(Some(m.obj))
			} 
			else if (lockval.get == m.txnid) {
				log.trace("Self-locked, replying to sender.")
				sender ! StoreProvider.GetResp(Some(m.obj))
			} else {
				// TODO: make a difference between this anomalous situation and the situation
				// where there is no object
				log.debug("Can not retrieve object because it is locked. | id = %s | val = %s", m.obj._id, lockval)
				sender ! StoreProvider.GetResp(Some(null))
			}
	}
}
*/