package org.hyflow.api

import scala.concurrent.stm._
import scala.reflect.AnyValManifest

object HRef {

	/**
	 * HRefFactory trait: for the actual implementation of HRef
	 */
	trait Factory {
		def newHRef(v0: Boolean, parent: HObj): HRef[Boolean]
		def newHRef(v0: Byte, parent: HObj): HRef[Byte]
		def newHRef(v0: Short, parent: HObj): HRef[Short]
		def newHRef(v0: Char, parent: HObj): HRef[Char]
		def newHRef(v0: Int, parent: HObj): HRef[Int]
		def newHRef(v0: Float, parent: HObj): HRef[Float]
		def newHRef(v0: Long, parent: HObj): HRef[Long]
		def newHRef(v0: Double, parent: HObj): HRef[Double]
		def newHRef(v0: Unit, parent: HObj): HRef[Unit]
		def newHRef[A: ClassManifest](v0: A, parent: HObj): HRef[A]
	}

	/**
	 * HRefMaker trait: for objects that create references
	 */
	trait Maker {
		// Self type: HRefMaker objects MUST also be HObj
		this: HObj =>

		protected def field[A](initialValue: A)(implicit om: OptManifest[A]): HRef[A] = om match {
			case m: AnyValManifest[_] => newPrimitiveHRef(initialValue, m.asInstanceOf[AnyValManifest[A]])
			case m: ClassManifest[_] => HRef.factory.newHRef(initialValue, this)(m.asInstanceOf[ClassManifest[A]])
			case _ => HRef.factory.newHRef[Any](initialValue, this).asInstanceOf[HRef[A]]
		}

		private def newPrimitiveHRef[A](initialValue: A, m: AnyValManifest[A]): HRef[A] = {
			(m.newArray(0).asInstanceOf[AnyRef] match {
				case _: Array[Int] => field(initialValue.asInstanceOf[Int])
				case _: Array[Boolean] => field(initialValue.asInstanceOf[Boolean])
				case _: Array[Byte] => field(initialValue.asInstanceOf[Byte])
				case _: Array[Short] => field(initialValue.asInstanceOf[Short])
				case _: Array[Char] => field(initialValue.asInstanceOf[Char])
				case _: Array[Float] => field(initialValue.asInstanceOf[Float])
				case _: Array[Long] => field(initialValue.asInstanceOf[Long])
				case _: Array[Double] => field(initialValue.asInstanceOf[Double])
				case _: Array[Unit] => field(initialValue.asInstanceOf[Unit])
			}).asInstanceOf[HRef[A]]
		}

		protected def field(initialValue: Boolean): HRef[Boolean] = HRef.factory.newHRef(initialValue, this)
		protected def field(initialValue: Byte): HRef[Byte] = HRef.factory.newHRef(initialValue, this)
		protected def field(initialValue: Short): HRef[Short] = HRef.factory.newHRef(initialValue, this)
		protected def field(initialValue: Char): HRef[Char] = HRef.factory.newHRef(initialValue, this)
		protected def field(initialValue: Int): HRef[Int] = HRef.factory.newHRef(initialValue, this)
		protected def field(initialValue: Long): HRef[Long] = HRef.factory.newHRef(initialValue, this)
		protected def field(initialValue: Float): HRef[Float] = HRef.factory.newHRef(initialValue, this)
		protected def field(initialValue: Double): HRef[Double] = HRef.factory.newHRef(initialValue, this)
		protected def field(initialValue: Unit): HRef[Unit] = HRef.factory.newHRef(initialValue, this)
		
		//TODO: Ref to another HObj (actual reference)
	}

	/**
	 * Plug in the implementation here.
	 */
	private[hyflow] var factory: HRef.Factory = null
}

/**
 * Extension of scala-stm's `Ref` trait for references within a globally available object.
 * In hyflow, use `HRef` instead of `Ref`
 */
trait HRef[T] extends Ref[T] {
	def parent: HObj
}

