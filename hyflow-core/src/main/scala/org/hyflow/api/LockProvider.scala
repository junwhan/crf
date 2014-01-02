package org.hyflow.api

import akka.dispatch.Future

object LockProvider {
	case class LockResp(fid: Handle.FID, result: Boolean) extends Message
}

trait LockProvider {
	def lock(obj: HObj, fid: Handle.FID, txnid: Long): Future[LockProvider.LockResp]
	def unlock(obj: HObj, fid: Handle.FID, txnid: Long)
}