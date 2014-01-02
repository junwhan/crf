package scala.concurrent.stm.haistm

import org.hyflow.api._

trait HaiInTxnRefOps {
	this: HaiInTxn =>

	def get[T](handle: Handle[T]): T = {
		// Check if element is in our write-set
		var crtLevel = internalCurrentLevel
		log.trace("Found level. | currentLevel = %s | this = %s", crtLevel, this)
		var wsval = crtLevel.wsGet(handle)
		while (wsval == None && crtLevel != null) {
			wsval = crtLevel.wsGet(handle)
			crtLevel = crtLevel.parLevel
		}
		internalCurrentLevel.rsRecRead(handle)
		val res = wsval.getOrElse(handle._hy_data) 
		log.trace("Getting field. | fid = %s | result = %s | from write-set = %s", handle._hy_id, res, wsval != None)
		res
	}

	def getWith[T, Z](handle: Handle[T], f: T => Z): Z = throw new AbstractMethodError
	def relaxedGet[T](handle: Handle[T], equiv: (T, T) => Boolean): T = throw new AbstractMethodError
	def unrecordedRead[T](handle: Handle[T]): Unit /*UnrecordedRead[T]*/ = throw new AbstractMethodError

	def set[T](handle: Handle[T], v: T): Unit = {
		// Add to write-set
		log.trace("Setting field. | fid = %s | value = %s", handle._hy_id, v)
		internalCurrentLevel.wsRecWrite(handle, v)
	}

	def swap[T](handle: Handle[T], v: T): T = throw new AbstractMethodError
	def trySet[T](handle: Handle[T], v: T): Boolean = throw new AbstractMethodError
	def compareAndSet[T](handle: Handle[T], before: T, after: T): Boolean = throw new AbstractMethodError
	def compareAndSetIdentity[T, R <: T with AnyRef](handle: Handle[T], before: R, after: T): Boolean = throw new AbstractMethodError
	def getAndTransform[T](handle: Handle[T], func: T => T): T = throw new AbstractMethodError
	def transformAndGet[T](handle: Handle[T], func: T => T): T = throw new AbstractMethodError
	def transformIfDefined[T](handle: Handle[T], pf: PartialFunction[T, T]): Boolean = throw new AbstractMethodError
	def getAndAdd(handle: Handle[Int], delta: Int): Int = throw new AbstractMethodError

	//////////// TxnLocal stuff

	// We store transactional local values in the write buffer by pretending
	// that they are proper handles, but their data and metadata aren't actually
	// backed by anything.

	/*def txnLocalFind(local: TxnLocalImpl[_]): Int = findWrite(local)
	def txnLocalGet[T](index: Int): T = getWriteSpecValue[T](index)
	def txnLocalInsert[T](local: TxnLocalImpl[T], v: T) { writeAppend(local, false, v) }
	def txnLocalUpdate[T](index: Int, v: T) { writeUpdate(index, v) }*/

}