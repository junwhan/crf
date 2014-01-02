package org.hyflow.api

import org.hyflow.core.util._
import org.hyflow.Hyflow
import scala.concurrent.stm._
import scala.concurrent.stm.impl.TxnContext
import org.eintr.loglady.Logging

trait Directory extends Logging {

	def open0(id: String)(implicit mt: MaybeTxn): Option[HObj]

	def open[T <: HObj](id: String)(implicit mt: MaybeTxn): T = {
		openMany[T](List(id)).apply(0)
	}

	def openMany0(ids: List[String])(implicit mt: MaybeTxn): Map[String, Either[HObj, Symbol]]

	def openMany[T <: HObj](ids: List[String])(implicit mt: MaybeTxn): List[T] = {
		// TODO: make sure to remove from cache invalidated objects
		val (cached, newids) = ids.unzip { id =>
			val cached = HyflowBackendAccess().findCachedObject[T](id)
			val newid = if (cached == None) id else null
			(cached, newid)
		}

		val lookedup = openMany0(newids)

		// Post-process
		(0 until ids.length).map { i =>
			if (cached(i) == None) {
				val id = ids(i)
				if (id != null) {
					val res = lookedup(id)
					res match {
						case Left(obj) =>
							HyflowBackendAccess().recordOpen(obj)
							obj.asInstanceOf[T]
						case Right(error) =>
							error match {
								case 'locked_object | 'lost_object =>
									Txn.findCurrent match {
										case Some(txn) =>
											// If we're in a transaction, rollback
											Txn.rollback(Txn.OptimisticFailureCause('cannot_open_locked_object, Some(ids(i))))(txn)
										// TODO TODO TODO TODO: What about the other objects from this openMany ?!?!
										case None =>
											null.asInstanceOf[T]
									}
								case _ =>
									null.asInstanceOf[T]
							}
					}
				} else
					null.asInstanceOf[T]
			} else
				cached(i).get
		} toList
	}

	def openMany_map[T <: HObj](ids: List[String])(implicit mt: MaybeTxn): Map[String, T] = {
		ids.zip(openMany[T](ids)).toMap
	}

	def register0(obj: HObj, ack: Boolean)(implicit mt: MaybeTxn)

	def register(obj: HObj, ack: Boolean = false)(implicit mt: MaybeTxn) {
		if (!HyflowBackendAccess().recordRegister(obj)) {
			log.trace("Before register object. | id = %s | _owner = %s", obj._id, obj._owner)
			Hyflow.store.put(obj)
			register0(obj, ack)
			log.trace("After register object. | id = %s | _owner = %s", obj._id, obj._owner)
		}
	}

	def delete0(id: String)(implicit mt: MaybeTxn)

	def delete(id: String)(implicit mt: MaybeTxn) {
		if (!HyflowBackendAccess().recordDelete(id))
			delete0(id)
	}

	// TODO: add support for
	// Unrecorded Reads
	// Releasable Reads
	// Other goodies from scala-stm

}