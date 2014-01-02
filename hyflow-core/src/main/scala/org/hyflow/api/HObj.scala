package org.hyflow.api

import scala.concurrent.stm._
import org.hyflow.core.directory.Tracker
import akka.actor.ActorRef
import org.hyflow.Hyflow

trait HObj extends Handle[HObj] with HRef.Maker with Serializable {
	// TODO: do we really want to give hard, definitive names to objects?
	// Maybe the object instance itself shouldn't care, and instead use 
	// the name given by the Directory/Locator/Map interface.

	// The object identifier, must be passed to the constructor of sub-classes
	def _id: String
	def _hy_id: Tuple2[String, Int] = (_id, -1)

	// Owner node
	// Do not touch var. (ugly) how can we protect this from user interference? 
	// private[motstm] won't work
	var _owner: ActorRef = null

	def isLocalObject: Boolean = _owner == Hyflow.mainActor

	// Object associated with handle
	def _hy_obj = this

	// Internal members (not for use by users, but we can't enforce this as it's used by
	// a different package (scala.concurrent.stm.motstm) so private[..] won't work

	// Keep track of fields
	private var _numFields: Int = 0
	// TODO: check fields are only added during construction (how?)
	def _addNewField: Int = {
		val res = _numFields
		_numFields += 1
		res
	}
	
	// Constructor to register object
	// TODO: choice not to register (so we can operate on it locally before we register it)
	Hyflow.dir.register(this)
}

