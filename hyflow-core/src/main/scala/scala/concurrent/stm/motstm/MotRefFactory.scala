package scala.concurrent.stm
package motstm

import org.hyflow.api._

trait MotRefFactory extends impl.RefFactory with HRef.Factory{

	// scala-stm interface, parent object not defined
	// TODO: do we want to keep this, or should we throw an exception?
	def newRef(v0: Boolean): Ref[Boolean] = new BooleanRef(v0, null)
	def newRef(v0: Byte): Ref[Byte] = new ByteRef(v0, null)
	def newRef(v0: Short): Ref[Short] = new ShortRef(v0, null)
	def newRef(v0: Char): Ref[Char] = new CharRef(v0, null)
	def newRef(v0: Int): Ref[Int] = new IntRef(v0, null)
	def newRef(v0: Float): Ref[Float] = new FloatRef(v0, null)
	def newRef(v0: Long): Ref[Long] = new LongRef(v0, null)
	def newRef(v0: Double): Ref[Double] = new DoubleRef(v0, null)
	def newRef(v0: Unit): Ref[Unit] = new GenericRef(v0, null)
	def newRef[T : ClassManifest](v0: T): Ref[T] = new GenericRef(v0, null)
	
	// hyflow interface, takes parent HObj as argument
	def newHRef(v0: Boolean, parent: HObj): HRef[Boolean] = new BooleanRef(v0, parent)
	def newHRef(v0: Byte, parent: HObj): HRef[Byte] = new ByteRef(v0, parent)
	def newHRef(v0: Short, parent: HObj): HRef[Short] = new ShortRef(v0, parent)
	def newHRef(v0: Char, parent: HObj): HRef[Char] = new CharRef(v0, parent)
	def newHRef(v0: Int, parent: HObj): HRef[Int] = new IntRef(v0, parent)
	def newHRef(v0: Float, parent: HObj): HRef[Float] = new FloatRef(v0, parent)
	def newHRef(v0: Long, parent: HObj): HRef[Long] = new LongRef(v0, parent)
	def newHRef(v0: Double, parent: HObj): HRef[Double] = new DoubleRef(v0, parent)
	def newHRef(v0: Unit, parent: HObj): HRef[Unit] = new GenericRef(v0, parent)
	def newHRef[A: ClassManifest](v0: A, parent: HObj): HRef[A] = new GenericRef(v0, parent)
	
	//TODO: what do we do with these?
	def newTxnLocal[A](init: => A,
	                     initialValue: InTxn => A,
	                     beforeCommit: InTxn => Unit,
	                     whilePreparing: InTxnEnd => Unit,
	                     whileCommitting: InTxnEnd => Unit,
	                     afterCommit: A => Unit,
	                     afterRollback: Txn.Status => Unit,
	                     afterCompletion: Txn.Status => Unit): TxnLocal[A] = throw new AbstractMethodError
	
	def newTArray[A : ClassManifest](length: Int): TArray[A] = throw new AbstractMethodError
	def newTArray[A : ClassManifest](xs: TraversableOnce[A]): TArray[A] = throw new AbstractMethodError
	
	def newTMap[A, B]: TMap[A, B] = throw new AbstractMethodError
	def newTMapBuilder[A, B] = throw new AbstractMethodError
	
	def newTSet[A]: TSet[A] = throw new AbstractMethodError
	def newTSetBuilder[A] = throw new AbstractMethodError
 
}