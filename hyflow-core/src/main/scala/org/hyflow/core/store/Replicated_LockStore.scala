package org.hyflow.core.store

import org.hyflow.api._
import org.hyflow.Hyflow

import scala.collection.mutable
import org.eintr.loglady.Logging

import akka.pattern.ask
import akka.actor._
import akka.util.duration._
import akka.util.Timeout
import akka.dispatch.Future
import scala.collection.mutable.ArrayBuffer
object Replicated_LockStore extends Service with Logging with LockProvider with StoreProvider {
	private implicit val timeout = Timeout(5 seconds)
	// store messages
	case class GetMsg(ids: List[String], txnid: Long, pessimistic: Boolean, isRead: Boolean) extends Message
	case class PutMsg(obj: HObj) extends Message
	case class LostObjMsg(id: String, ver: Long, txnid: Long) extends Message
	case class ValidateMsg(fids: List[Handle.FID], txnid: Long) extends Message
  case class CheckMsg(id: String, txid: Long) extends Message
	// lock messages
	case class LockReqMsg(fid: Handle.FID, txnid: Long) extends Message
	case class LockReleaseMsg(fid: Handle.FID, txnid: Long) extends Message

	// hyflow service stuff 
	val name = "replicated-lock-store"
	val actorProps = Props[Replicated_LockStore_Actor]
	def accepts(message: Any): Boolean = message match {
		case _: GetMsg => true
		case _: PutMsg => true
    case _: CheckMsg => true
		case _: ValidateMsg => true
		case _: LostObjMsg => true
		case _: LockReqMsg => true
		case _: LockReleaseMsg => true
		case _ => false
	}

	def get(ids: List[String], txnid: Long, peer: ActorRef, pessimistic: Boolean, isRead: Boolean): Future[StoreProvider.GetResp] = {
		ask(peer, GetMsg(ids, txnid, pessimistic, isRead)).asInstanceOf[Future[StoreProvider.GetResp]]
	}

	def put(obj: HObj) {
		ref ! PutMsg(obj)
	}
	
	def lost(obj: HObj, txnid: Long) {
		obj._owner ! LostObjMsg(obj._id, obj._hy_ver, txnid) 
	}
	
	def validate(peer: ActorRef, handles: List[Handle[_]], txnid: Long): Future[StoreProvider.ValidateResp] = {
		ask(peer, ValidateMsg(handles.map(_._hy_id), txnid)).asInstanceOf[Future[StoreProvider.ValidateResp]]
	}
  def check(obj: HObj, txid: Long): Future[StoreProvider.CheckResp] = {
    ask(obj._owner, CheckMsg(obj._id, txid)).asInstanceOf[Future[StoreProvider.CheckResp]]
  }
	def lock(obj: HObj, fid: Handle.FID, txnid: Long): Future[LockProvider.LockResp] = {
		ask(obj._owner, LockReqMsg(fid, txnid)).asInstanceOf[Future[LockProvider.LockResp]]
	}
	
	def unlock(obj: HObj, fid: Handle.FID, txnid: Long) {
		obj._owner ! LockReleaseMsg(fid, txnid)
	}
}

class Replicated_LockStore_Actor extends Actor with Logging {
	
	import Replicated_LockStore._
	
	private class ObjectStoreEntry {
		var obj: HObj = null
		// TODO: so far only single-object lock
    val versionList = new ArrayBuffer[HObj]();
		var lockedBy: Long = 0
	}

  private val objectQ = mutable.Map[String, mutable.ListBuffer[Long]]()  // TxEntry
  private val transactQ = mutable.Map[Long, mutable.ListBuffer[String]]() // ObjEntry
	private val store = mutable.Map[String, ObjectStoreEntry]()
	
	def receive() = {
		case m: GetMsg =>
			val resp = m.ids.map { id =>
				val entryOpt = store.get(id)
				val res: Either[HObj, Symbol] = if (entryOpt == None) {
					log.warn("Object store queried for unknown object. | id = %s | sender = %s", id, sender)
					Right('unknown_object)
				} else {
					val entry = entryOpt.get
					if (entry.obj == null) {
						log.debug("Object store queried for an unavailable (lost) object. | id = %s | sender = %s", 
								id, sender)
						Right('lost_object)
					} else if (entry.lockedBy == 0) {
						log.debug("Retrieving object from store. | id = %s | sender = %s | obj = %s | pessimistic = %s", 
								id, sender, entry.obj, m.pessimistic)
						if (m.pessimistic) {
							entry.lockedBy = m.txnid
						}
						Left(entry.obj)
					} else if (entry.lockedBy != m.txnid) {
						log.debug("Object store queried for a locked object. | id = %s | sender = %s", id, sender)
						Right('locked_object)
					} else {
						log.warn("Object store queried for a locked object by the lock holder. | id = %s | sender = %s",
								id, sender)
						Left(entry.obj)
					}
				}
				(id, res)
			}
			sender ! StoreProvider.GetResp(resp)
			
		case m: PutMsg =>
			val entry = store.getOrElseUpdate(m.obj._id, new ObjectStoreEntry)
			// TODO: maybe sanity check that entry is not locked by another txn
			m.obj._owner = Hyflow.mainActor
      //m.obj._owner = Hyflow.peers(0)
			entry.obj = m.obj
			log.debug("Storing object locally. | id = %s | obj = %s | _owner = %s", 
					m.obj._id, m.obj, m.obj._owner)
			
		case m: LostObjMsg =>
			val entry = store(m.id)
			if (m.ver < entry.obj._hy_ver) {
				log.debug("Received stale lost object message. Ignoring. | id = %s | sender = %s", m.id, sender)
			} else {
				log.debug("Object ownership was lost. | id = %s | sender = %s", m.id, sender)
				entry.obj = null
				entry.lockedBy = 0
			}
		
		case m: ValidateMsg =>
			// TODO: This only works for object versions (initial implementation)
			val res = for (fid <- m.fids) 
				yield (fid, store.get(fid._1)
					.filter(x => x.obj != null && (x.lockedBy == m.txnid || x.lockedBy == 0))
					.map(_.obj._hy_ver))
			log.debug("Validating objects for remote node. | sender = %s | res = %s", sender, res)
			sender ! StoreProvider.ValidateResp(res.toMap)
			
		case m: LockReqMsg =>
			val entry = store(m.fid._1)
			val granted = if (entry.obj == null) {
				log.debug("Lock request rejected (object unavailable). | fid = %s", m.fid)
				false
			} else if (entry.lockedBy == 0 || entry.lockedBy == m.txnid) {
				log.debug("Lock request granted. | fid = %s | re-entry = %s", 
						m.fid, entry.lockedBy == m.txnid)
				entry.lockedBy = m.txnid
				true
			} else {
				log.debug("Lock request rejected (object locked). | fid = %s", m.fid)
				false
			}
			sender ! LockProvider.LockResp(m.fid, granted)
			
		case m: LockReleaseMsg => 
			val entry = store(m.fid._1)
			if (m.txnid == entry.lockedBy) {
				log.debug("Lock released for local object. | fid = %s", m.fid)
			} else if (entry.lockedBy != 0) {
				log.warn("Lock released for local object by wrong txnid. | fid = %s | lockedBy = %s | txnid = %s", 
						m.fid, entry.lockedBy, m.txnid)
			} else {
				log.debug("WARN: Unlocking unlocked object. | fid = %s", m.fid)
			}
			entry.lockedBy = 0
			
	}
}