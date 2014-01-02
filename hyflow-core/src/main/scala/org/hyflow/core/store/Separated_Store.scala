package org.hyflow.core.store
/*
import org.hyflow.api._
import org.hyflow.Hyflow

import scala.collection.mutable
import org.eintr.loglady.Logging

import akka.dispatch.Future
import akka.pattern.ask
import akka.actor._
import akka.util.duration._
import akka.util.Timeout

object Separated_Store extends Service with Logging with StoreProvider {
	
	private implicit val timeout = new Timeout(5 seconds)
	
	// messages
	case class GetMsg(id: String, txnid: Long, pessimistic: Boolean) extends Message
	case class PutMsg(obj: HObj) extends Message
	case class LostObjMsg(id: String, txnid: Long) extends Message
	case class ValidateMsg(fids: List[Handle.FID], txnid: Long) extends Message

	// hyflow service stuff 
	val name = "separated-store"
	val actorProps = Props[Separated_Store_Actor]
	def accepts(message: Any): Boolean = message match {
		case _: GetMsg => true
		case _: PutMsg => true
		case _: ValidateMsg => true
		case _: LostObjMsg => true
		case _ => false
	}
	
	def get(id: String, txnid: Long, peer: ActorRef, pessimistic: Boolean): Future[StoreProvider.GetResp] = {
		ask(peer, GetMsg(id, txnid, pessimistic)).asInstanceOf[Future[StoreProvider.GetResp]]
	}

	def put(obj: HObj) {
		ref ! new PutMsg(obj)
	}
	
	def lost(obj: HObj, txnid: Long) {
		obj._owner ! LostObjMsg(obj._id, txnid) 
	}
	
	def validate(peer: ActorRef, handles: List[Handle[_]], txnid: Long): Future[StoreProvider.ValidateResp] = {
		ask(peer, ValidateMsg(handles.map(_._hy_id), txnid)).asInstanceOf[Future[StoreProvider.ValidateResp]]
	}
}

// TODO: update this to hold multiple versions. (how/where?)
// TODO: (IMPORTANT) check that the object doesn't get updated elsewhere while in store/transit
class Separated_Store_Actor extends Actor with Logging {
	import Separated_Store._

	val store = mutable.Map[String, HObj]()

	def receive() = {
		case m: GetMsg =>
			// TODO: don't give out object if it is already locked!
			val obj = store.get(m.id)
			log.debug("Retrieving object from store. | id = %s | sender = %s | obj = %s", m.id, sender, obj)
			if (obj == None || obj.get == null)
				sender ! StoreProvider.GetResp(obj)
			else {
				// TODO: forward to lock holder, if lock holder may be on a different node 
				// account for lock holder location strategy
				log.trace("Forwarding message to lockholder.")
				Separated_Lock.ref.forward(Separated_Lock.EnsureUnlockedObj(m.txnid, obj.get, m.pessimistic))
			}
		case m: PutMsg =>
			m.obj._owner = Hyflow.mainActor
			log.debug("Storing object locally. | id = %s | obj = %s | _owner = %s", 
					m.obj._id, m.obj, m.obj._owner)
			store.put(m.obj._id, m.obj)
			sender ! "ok"
		case m: ValidateMsg =>
			log.debug("Validating objects for remote node. | ids = %s | sender = %s", m.fids, sender)
			// TODO: how do we retrieve field version?
			// This only works for object versions (initial implementation)
			val res = for (fid <- m.fids) yield (fid, store.get(fid._1).filter(_ != null).map(_._hy_ver))
			// Pass this to the lock holder to blank out locked objects
			log.trace("Forwarding validation to LockHolder. | vers = %s", res)
			Separated_Lock.ref.forward(Separated_Lock.EnsureUnlockedVer(m.txnid, res.toMap))
			//sender ! new ValidateRespMsg(res.toMap)
		case m: LostObjMsg =>
			log.debug("Object ownership was lost. | id = %s | sender = %s", m.id, sender)
			// So a reply of Some(null) means either: object locked or object not here anymore 
			store.put(m.id, null)
			// Release any locks we may hold
			// TODO: what about field locks?
			Separated_Lock.ref.forward(Separated_Lock.LockReleaseMsg((m.id, -1), m.txnid))
		case m: Separated_Lock.LockReqMsg =>
			log.debug("Checking if object is still here. (for acquiring the lock). | id = %s", m.fid._1)
			store.get(m.fid._1) match {
				case Some(null) | None =>
					sender ! LockProvider.LockResp(m.fid, false)
				case Some(obj) =>
					Separated_Lock.ref.forward(Separated_Lock.LockReqPh2Msg(m))
			}
	}
}
*/