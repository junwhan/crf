package scala.concurrent.stm
package motstm

import org.hyflow.api.Handle

private[motstm] trait RefOps[T] extends Ref[T] { 
	
	def handle: Handle[T]
	private def impl(implicit txn: InTxn): MotInTxn = txn.asInstanceOf[MotInTxn]
	
	/////////////// Source stuff
	override def apply()(implicit txn: InTxn): T = impl.get(handle)
	def get(implicit txn: InTxn): T = impl.get(handle)
	
	def getWith[Z](f: (T) => Z)(implicit txn: InTxn): Z = throw new AbstractMethodError
	def relaxedGet(equiv: (T, T) => Boolean)(implicit txn: InTxn): T = throw new AbstractMethodError
	
	//////////////// Sink stuff
	override def update(v: T)(implicit txn: InTxn) { impl.set(handle, v) }
	def set(v: T)(implicit txn: InTxn) { impl.set(handle, v) }
	def trySet(v: T)(implicit txn: InTxn): Boolean = throw new AbstractMethodError
	
	//////////////// Ref stuff
	def swap(v: T)(implicit txn: InTxn): T = throw new AbstractMethodError
	def transform(f: T => T)(implicit txn: InTxn) = throw new AbstractMethodError
	def transformIfDefined(pf: PartialFunction[T,T])(implicit txn: InTxn): Boolean = throw new AbstractMethodError
}