package org.hyflow.api

import akka.actor._
import akka.dispatch.Future

object StoreProvider {
	case class GetResp(objects: List[Tuple2[String,Either[HObj,Symbol]]]) extends Message
	case class ValidateResp(vers: Map[Handle.FID, Option[Long]]) extends Message
  case class CheckResp(nTx: Option[Int]) extends Message
}

trait StoreProvider {
	def get(ids: List[String], txnid: Long, peer: ActorRef, pessimistic: Boolean, isRead: Boolean): Future[StoreProvider.GetResp]
	def put(obj: HObj)
	def validate(peer: ActorRef, handles: List[Handle[_]], txnid: Long): Future[StoreProvider.ValidateResp]
	//TODO: delete!
	def lost(obj: HObj, txnid: Long)
  def check(obj: HObj, txid : Long): Future[StoreProvider.CheckResp]
}
