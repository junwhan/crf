package org.hyflow.api

import org.hyflow.api._
import scala.concurrent.stm._

object HyflowBackendAccess  {
	var backend: HyflowBackendAccess = _
	
	def configure(_backend: HyflowBackendAccess) {
		backend = _backend
	}
	
	def apply() = backend
}

trait HyflowBackendAccess {
	def isPessimisticMode(implicit mt: MaybeTxn): Boolean
	def recordOpen(obj: HObj)(implicit mt: MaybeTxn): Boolean
	def recordRegister(obj: HObj)(implicit mt: MaybeTxn): Boolean
	def recordDelete(id: String)(implicit mt: MaybeTxn): Boolean
	def getCrtTxnId(implicit mt: MaybeTxn): Long
	def forward(rclk: Long)(implicit mt: MaybeTxn)
	def findCachedObject[T <: HObj](id: String)(implicit mt: MaybeTxn): Option[T]
}