package org.hyflow.api
import org.hyflow.Hyflow


/**
 * Base class for all Hyflow messages
 * Implement support for payloads and call for serialization using protostuff 
 */
abstract class Message extends Serializable {
	
	// Payloads are initialized by simply asking the handlers for a value.
	// The message is not passed to the handler anymore.
	val payloads: Map[String,Any] = ( 
		for {
			handler <- Hyflow.payloads; 
			value = handler.outgoing; 
			if value != None
		} yield handler.name -> value.get).toMap
	
}
