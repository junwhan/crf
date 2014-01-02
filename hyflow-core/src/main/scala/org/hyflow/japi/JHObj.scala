package org.hyflow.japi

import org.hyflow.api._
import scala.concurrent.stm._

abstract class JHObj extends HObj {
	protected def jfield[A](initialValue: A): Ref.View[A] = field(initialValue).single
	protected def jfield(initialValue: Boolean): Ref.View[Boolean] = field(initialValue).single
	protected def jfield(initialValue: Byte): Ref.View[Byte] = field(initialValue).single
	protected def jfield(initialValue: Short): Ref.View[Short] = field(initialValue).single
	protected def jfield(initialValue: Char): Ref.View[Char] = field(initialValue).single
	protected def jfield(initialValue: Int): Ref.View[Int] = field(initialValue).single
	protected def jfield(initialValue: Long): Ref.View[Long] = field(initialValue).single
	protected def jfield(initialValue: Float): Ref.View[Float] = field(initialValue).single
	protected def jfield(initialValue: Double): Ref.View[Double] = field(initialValue).single
}
