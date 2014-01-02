package org.hyflow.api
import akka.actor._

// TODO: do we want multi-thread servicing of messages? 
// If so, check how to enable round-robin routing here

trait Service {
	val name: String
	val actorProps: Props
	def accepts(message : Any) : Boolean 
	
	// Do not override this field:
	final var ref: ActorRef = null
}
