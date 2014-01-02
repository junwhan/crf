package org.hyflow.api

/**
 * Appends payloads to outgoing messages, and processes them on incoming messages.
 * All methods of this interface must be reentrant!
 */
trait PayloadHandler {
	val name: String
	def outgoing: Option[Any]
	def incoming(hymsg: Message): Unit
}