package scala.concurrent.stm.haistm

import scala.concurrent.stm._
import scala.actors.threadpool.TimeUnit

object HaiSTM extends HaiTxnExecutor with impl.STMImpl with HaiRefFactory {
	
	// TxnContext trait, delegate context lookups to MotInTxn object
	def findCurrent(implicit mt: MaybeTxn): Option[InTxn] = Option(HaiInTxn.currentOrNull)
	def dynCurrentOrNull: InTxn = HaiInTxn.dynCurrentOrNull

	/** Hashes `base` with `offset`. */
	def hash(base: AnyRef, offset: Int): Int = hash(base) ^ (0x40108097 * offset)

	/** Hashes `base` using its identity hash code. */
	def hash(base: AnyRef): Int = {
		val h = System.identityHashCode(base)
		// multiplying by -127 is recommended by java.util.IdentityHashMap
		h - (h << 7)
	}

	// Commit barrier
	def newCommitBarrier(timeout: Long, unit: TimeUnit): CommitBarrier = throw new AbstractMethodError
}
