package org.hyflow.core

import org.hyflow.api._
import org.hyflow.Hyflow
import org.hyflow.core.util._
import akka.actor._
import akka.util.duration._
import scala.collection.mutable.ListBuffer
import org.eintr.loglady.Logging

object LifecycleManager extends Service {
	//classes
	case class HelloMsg(me: ActorRef) extends Message {
		def this() = this(null)
	}
	case class BeginMsg(peers: List[ActorRef]) extends Message
	case class DoneMsg() extends Message
	case class ByeMsg() extends Message

	val name = "lifecycle-manager"
	val actorProps = Props[LifecycleManager]
	def accepts(message: Any): Boolean = message match {
		case _: HelloMsg => true
		case _: BeginMsg => true
		case _: DoneMsg => true
		case _: ByeMsg => true
		case _ => false
	}
}

class LifecycleManager extends Actor with Logging {
	import LifecycleManager._

	log.info("Initializing lifecycle manager.")
	var count: Int = 0
	val initialPeers = ListBuffer[ActorRef]()

	def receive() = {
		case m: HelloMsg =>
			// Only received by coordinator node.
			log.debug("Received hello message. | sender = %s | m.me = %s", sender, m.me)
			initialPeers.prepend(m.me)
			count += 1
			if (count == HyflowConfig.cfg[Int]("hyflow.nodes")) {
				
				
				// TODO: REMOVE Profiling code!!!!
				val profilerWait = HyflowConfig.cfg[Int]("hyflow.profilerWait")
				println("Will wait %d seconds to attach profiler.".format(profilerWait))
				//
				/*context.system.scheduler.schedule(10 seconds, 10 seconds) {
					for (peer <- initialPeers)
						peer ! "ping"
				}*/
				
				context.system.scheduler.scheduleOnce(profilerWait seconds) {
					log.debug("Waiting done. Broadcasting start message and list of peers.")
					// TODO: Why do the peers get rearranged?!? (only with checkpointing)
					val bgnmsg = new BeginMsg(initialPeers.toList)
					for (peer <- initialPeers)
						peer ! bgnmsg
				}
			}
		case m: BeginMsg =>
			log.debug("Received begin message. Listing peers.")
			// Only sent by coordinator node. (Received by all)
			// Retrieve peers list from coordinator
			Hyflow.peers.clear()
			Hyflow.peers ++= m.peers
			count = Hyflow.peers.length
			val myIdx = Hyflow.peers.indexOf(Hyflow.mainActor)
			for (peer <- Hyflow.peers) {
				val idx = Hyflow.peers.indexOf(peer)
				log.debug("| peer = %s", peer)
				if (myIdx > idx)
					context.system.scheduler.scheduleOnce((Math.random * 3000).toInt milliseconds) {
						peer ! "ping"
					}
			}
			// 
			context.system.scheduler.scheduleOnce(4 seconds) {
				Hyflow.callOnReady()
			}

		case m: DoneMsg =>
			// Sent by all to coordinator
			count -= 1
			if (count == 0) {
				val byemsg = new ByeMsg
				for (peer <- Hyflow.peers)
					peer ! byemsg
			}

		case m: ByeMsg =>
			// shutdown now
			Hyflow.shutdown()
	}
}
