package org.hyflow.core.directory

import org.hyflow.api._
import org.hyflow.core.util._
import org.hyflow.core.store._
import org.hyflow.Hyflow
import akka.pattern.ask
import akka.util.duration._
import akka.dispatch._
import akka.actor._
import scala.collection.mutable
import scala.concurrent.stm._
import org.eintr.loglady.Logging
import scala.concurrent.stm.motstm.TFAClock

object Tracker extends Tracker

object MessagesTracker {
	// classes 
	case class LocateMsg(ids: List[String]) extends Message
	case class RegisterMsg(id: String, ver: Long, owner: ActorRef) extends Message
	case class UpdateVerMsg(id: String, ver: Long) extends Message
	case class DeleteMsg(id: String) extends Message
	case class LocateRespMsg(results: List[Tuple2[String, Option[ActorRef]]]) extends Message
	case class ReqUpdNotifMsg(id: String, minver: Long) extends Message
	case class UpdNotifMsg() extends Message
}

private[directory] class Tracker extends Directory with Service with Logging {
	import MessagesTracker._

	// Hyflow service stuff
	val name = "tracker"
	val actorProps = Props[TrackerActor]
	def accepts(message: Any): Boolean = message match {
		case _: LocateMsg => true
		case _: RegisterMsg => true
		case _: DeleteMsg => true
		case _: ReqUpdNotifMsg => true
		case _: UpdateVerMsg => true
		case _ => false
	}

	// Directory stuff
	def responsiblePeer(id: String) = {
		val peers = Hyflow.peers
		log.trace("Locating responsible peer for object. | id=%s | id ## = %s | peers.size = %s | index = %s",
			id, id ##, peers.size, math.abs(id ##) % peers.size)
		peers(math.abs(id ##) % peers.size)
	}

	def open0(id: String)(implicit mt: MaybeTxn): Option[HObj] = {
		openMany0(List(id)).apply(id).left.toOption
	}

	def openMany0(ids: List[String])(implicit mt: MaybeTxn): Map[String,Either[HObj,Symbol]] = {
		// TODO: locate multiple messages in one message
		implicit val executor: ExecutionContext = Hyflow.system.dispatcher

		// Do this in stages, so a minimal number of messages need to be sent at each
		// stage (i.e. one for each host).
		
		// Stage 1: retrieve owners
		// Collect hosts
		val trackerPairs = ids.map(id => (id, responsiblePeer(id)))
		val trackerSet = mutable.Map[ActorRef, mutable.ListBuffer[String]]()
		for (pair <- trackerPairs) {
			val (id, tracker) = pair
			if (id != null) {
				val requests = trackerSet.getOrElseUpdate(tracker, mutable.ListBuffer())
				requests.append(id)
			}
		}
		// Send messages
		val owner_futures = trackerSet.toMap.map { args =>
			val (tracker, reqs) = args
			val msg = LocateMsg(reqs.toList)
			tracker.ask(msg)(5 seconds).asInstanceOf[Future[LocateRespMsg]]
		}
		// Wait for owners
		val owner_all_f = Future.sequence(owner_futures)
		val owners = Await.result(owner_all_f, 5 seconds)

		
		// Stage 2: retrieve objects
		// Collect hosts / owners
		val ownerSet = mutable.Map[ActorRef, mutable.ListBuffer[String]]()
		for (ans <- owners) {
			TFAClock.incoming(ans)
			for (res <- ans.results) {
				val (id, owner) = res
				if (owner != None && owner.get != null) {
					val requests = ownerSet.getOrElseUpdate(owner.get, mutable.ListBuffer())
					requests.append(id)
				}
			}
		}
		// Send messages
		val txnid = HyflowBackendAccess().getCrtTxnId
		val obj_futures = ownerSet.toMap.map { args =>
			val (owner, reqs) = args
			Hyflow.store.get(reqs.toList, txnid, owner, HyflowBackendAccess().isPessimisticMode, false)
      //Hyflow.store.get(reqs.toList, txnid, Hyflow.peers(0), HyflowBackendAccess().isPessimisticMode)
		}
		// Wait for objects
		val obj_all_f = Future.sequence(obj_futures)
		val obj_all = Await.result(obj_all_f, 5 seconds)
		
		
		// Post-processing / Finish up
		val res = mutable.Map[String, Either[HObj,Symbol]]()
		for (ans <- obj_all) {
			if (ans.objects.length > 0) {
				// Forward transactions as needed.
				TFAClock.incoming(ans) //TODO: !!!
				val rclk = ans.payloads("tfaclock").asInstanceOf[Long] // !!!
				// TODO: fix(txn forwarding)
				if (txnid != 0) {
					HyflowBackendAccess().forward(rclk)
				}
				// TODO: end fix
				
				for (obj_pair <- ans.objects) {
					val (id, obj_e) = obj_pair
					res.put(id, obj_e)
				}
			}
		}
		res.toMap
	}

	def register0(obj: HObj, ack: Boolean)(implicit mt: MaybeTxn) {
		val tracker = responsiblePeer(obj._id)
		log.trace("Sending a message")
		tracker ! RegisterMsg(obj._id, obj._hy_ver, Hyflow.mainActor)

		if (ack == true)
			throw new AbstractMethodError("Acknowledge on registration not implemented.")

		/*if (ack) {
			val future = tracker.ask(RegisterMsg(obj._id, obj._hy_ver, Hyflow.mainActor))(5 seconds)
			Await.result(future, 5 seconds)
		} else {
			tracker ! new RegisterMsg(obj._id, obj._hy_ver, Hyflow.mainActor)
		}*/
	}

	// TODO: delete from store as well
	def delete0(id: String)(implicit mt: MaybeTxn) {
		val tracker = responsiblePeer(id)
		tracker ! new DeleteMsg(id)
	}
}

private[directory] class TrackerActor extends Actor with Logging {
	import MessagesTracker._

	private class TrackerEntry(val id: String) {
		var owner: ActorRef = null
		var ver: Long = 0L
		var notifReqs = mutable.ListBuffer[ActorRef]()
	}
	private val tracked = mutable.Map[String, TrackerEntry]()

	def receive() = {
		case m: LocateMsg =>
			val res = m.ids.map(id => (id, tracked.get(id).map(_.owner)))
			//val res = tracked.get(m.id).map(_.owner)
			log.debug("Querying tracker. | sender = %s | res = %s", sender, res)
			sender ! LocateRespMsg(res)

		case m: RegisterMsg =>
			log.debug("Registering object. | id = %s | owner = %s | ver = %d", m.id, m.owner, m.ver)
			val entry = tracked.getOrElseUpdate(m.id, new TrackerEntry(m.id))
			if (m.ver >= entry.ver) {
				entry.ver = m.ver
				entry.owner = m.owner
        //entry.owner = Hyflow.peers(0)
				if (!entry.notifReqs.isEmpty) {
					// Notify all and clear
					entry.notifReqs.foreach(_.tell(m.id))
					entry.notifReqs.clear()
				}
				// Confirm
				sender ! "ok"
			} else {
				log.warn("Ignoring out-of-order object registration. | id = %s | entry.ver = %s | m.ver = %s",
					m.id,
					entry.ver,
					m.ver)
				sender ! "err"
			}

		case m: UpdateVerMsg =>
			log.debug("Updating object version. | id = %s | ver = %s", m.id, m.ver)
			val entry = tracked.get(m.id).get // It MUST exist (unless deleted by someone else)
			if (m.ver > entry.ver) {
				entry.ver = m.ver
				if (!entry.notifReqs.isEmpty) {
					// Notify all and clear
					entry.notifReqs.foreach(_.tell(m.id))
					entry.notifReqs.clear()
				}
			}

		case m: DeleteMsg =>
			log.debug("Deleting object. | id = %s | sender = %s", m.id, sender)
			val oldEntry = tracked.remove(m.id)
			if (oldEntry != None) {
				// Notify all
				oldEntry.get.notifReqs.foreach(_.tell(m.id))
			}
			sender ! "ok"

		case m: ReqUpdNotifMsg =>
			log.debug("Remote party requested to be notified when object gets updated. | id = %s | minver = %d",
				m.id, m.minver)
			// Check if object is already newer
			val entry = tracked(m.id)
			if (entry.ver > m.minver) {
				// Reply right away
				sender ! m.id
			} else {
				// Remember for later
				entry.notifReqs += sender
			}

		//case _@ m =>
		//	log.debug("Unexpected message. | m = %s", m)
	}
}