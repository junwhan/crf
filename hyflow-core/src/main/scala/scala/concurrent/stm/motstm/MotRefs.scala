package scala.concurrent.stm
package motstm

import org.hyflow.api._

// move BaseRef into hyflow scope, to keep HObj._addNewField private
private abstract class BaseRef[A](val parent: HObj) extends Handle[A] with HRef[A] with RefOps[A] with ViewOps[A] {

	// constructor: keep track of location id within parent
	if (parent == null) {
		throw new Exception("In Hyflow, `Ref`s must be initialized using HObj.field(...) instead of Ref(...)")
	}
	val refChildId = parent._addNewField
	// end constructor

	def handle: Handle[A] = this
	def single: Ref.View[A] = this
	def ref: Ref[A] = this

	// Base+Offset to support arrays, otherwise offset==0 
	def base: AnyRef = this
	def offset: Int = 0
	
	// Override handle
	val _hy_id: Tuple2[String, Int] = (parent._id, refChildId)
	val _hy_obj = parent
	
}

//@volatile or not?
private class BooleanRef(override var _hy_data: Boolean, parent: HObj) extends BaseRef[Boolean](parent)
private class ByteRef(override var _hy_data: Byte, parent: HObj) extends BaseRef[Byte](parent)
private class ShortRef(override var _hy_data: Short, parent: HObj) extends BaseRef[Short](parent)
private class CharRef(override var _hy_data: Char, parent: HObj) extends BaseRef[Char](parent)

private class IntRef(override var _hy_data: Int, parent: HObj) extends BaseRef[Int](parent) {
	override def +=(rhs: Int)(implicit num: Numeric[Int]) { incr(rhs) }
	override def -=(rhs: Int)(implicit num: Numeric[Int]) { incr(-rhs) }
	private def incr(delta: Int) {
		if (delta != 0) {
			MotInTxn.dynCurrentOrNull match {
				case null => NonTxn.getAndAdd(handle, delta)
				case txn => txn.getAndAdd(handle, delta)
			}
		}
	}
}

private class FloatRef(override var _hy_data: Float, parent: HObj) extends BaseRef[Float](parent)
private class LongRef(override var _hy_data: Long, parent: HObj) extends BaseRef[Long](parent)
private class DoubleRef(override var _hy_data: Double, parent: HObj) extends BaseRef[Double](parent)
private class GenericRef[A](override var _hy_data: A, parent: HObj) extends BaseRef[A](parent) 
