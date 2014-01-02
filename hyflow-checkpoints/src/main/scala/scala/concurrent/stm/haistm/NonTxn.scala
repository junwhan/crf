/* scala-stm - (c) 2009-2011, Stanford University, PPL */

package scala.concurrent.stm
package haistm

import org.hyflow.api._
/**
 * The object that contains the code for non-transactional read and write
 *  barriers.
 *
 *  @author Nathan Bronson
 */
private[haistm] object NonTxn {

	// All refs originally Handle[T]
	
	def get[T](Ref: Handle[T]): T = throw new AbstractMethodError

	def await[T](Ref: Handle[T], pred: T => Boolean) = throw new AbstractMethodError

	def tryAwait[T](Ref: Handle[T], pred: T => Boolean, timeoutNanos: Long): Boolean = throw new AbstractMethodError

	def set[T](Ref: Handle[T], v: T) = throw new AbstractMethodError

	def swap[T](Ref: Handle[T], v: T): T = throw new AbstractMethodError

	def trySet[T](Ref: Handle[T], v: T): Boolean = throw new AbstractMethodError

	def compareAndSet[T](Ref: Handle[T], before: T, after: T): Boolean = throw new AbstractMethodError

	def compareAndSetIdentity[T, R <: AnyRef with T](Ref: Handle[T], before: R, after: T): Boolean = throw new AbstractMethodError

	def transformAndGet[T](Ref: Handle[T], f: T => T): T = throw new AbstractMethodError

	def getAndTransform[T](Ref: Handle[T], f: T => T): T = throw new AbstractMethodError

	def transformIfDefined[T](Ref: Handle[T], pf: PartialFunction[T, T]): Boolean = throw new AbstractMethodError

	def getAndAdd(Ref: Handle[Int], delta: Int): Int = throw new AbstractMethodError
}

