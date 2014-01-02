package org.hyflow.api

object Handle {
	type FID = Tuple2[String, Int]
}

trait Handle[T] extends Serializable {
	//@volatile
	var _hy_ver: Long = 0
	// _hy_data getter and setter. Setter needed for override vars to work down the line.
	def _hy_data: T = this.asInstanceOf[T]
	def _hy_data_=(v: T) = () 
	// Object/Ref Identifier
	def _hy_id: Handle.FID
	// Hash by identifier
	def _hy_hash: Int = _hy_id ##
	// HObj associated with this handle
	def _hy_obj : HObj
}