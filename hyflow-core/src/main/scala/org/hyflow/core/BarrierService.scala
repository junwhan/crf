package org.hyflow.core

import org.hyflow.api._
import akka.actor._
import scala.collection.mutable
import org.eintr.loglady.Logging

object BarrierService extends Service with Logging {
	case class BarrierMsg(id: String, target: Int) extends Message
	case class BarrierRespMsg() extends Message

	val name = "barrier-service"
	val actorProps = Props[BarrierService]
	def accepts(message: Any): Boolean = message match {
			case m: BarrierMsg => true
			case _ => false
		}
}

// TODO: Barrier timeout?
private class BarrierService extends Actor {
	case class BarrierEntry(
		var count: Int = 0,
		val waiters: mutable.ListBuffer[ActorRef] = mutable.ListBuffer())

	import BarrierService._

	val entries = mutable.Map[String, BarrierEntry]()

	def receive() = {
		case m: BarrierMsg =>
			val entry = entries.getOrElseUpdate(m.id, BarrierEntry())
			entry.count += 1
			entry.waiters += sender
			if (entry.count >= m.target) {
				entry.waiters.foreach(_ ! BarrierRespMsg())
				entries.remove(m.id)
			}
	}
}