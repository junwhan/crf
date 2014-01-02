package scala.concurrent.stm.haistm

import scala.concurrent.stm._
import scala.util.control.ControlThrowable
import org.hyflow.api._


private[haistm] object HaiTxnExecutor {
	val DefaultControlFlowTest = { x: Throwable => x.isInstanceOf[ControlThrowable] }

	val DefaultPostDecisionFailureHandler = { (status: Txn.Status, x: Throwable) =>
		new Exception("status=" + status, x).printStackTrace()
	}
}

private[haistm] case class HaiTxnExecutor private (
	retryTimeoutNanos: Option[Long],
	controlFlowTest: Throwable => Boolean,
	postDecisionFailureHandler: (Txn.Status, Throwable) => Unit) extends TxnExecutor {

	def this() = this(None, HaiTxnExecutor.DefaultControlFlowTest, HaiTxnExecutor.DefaultPostDecisionFailureHandler)

	def apply[Z](block: InTxn => Z)(implicit mt: MaybeTxn): Z = HaiInTxn().atomic(this, block)
	
	def oneOf[Z](blocks: (InTxn => Z)*)(implicit mt: MaybeTxn): Z = throw new AbstractMethodError
	def pushAlternative[Z](mt: MaybeTxn, block: InTxn => Z): Boolean = throw new AbstractMethodError
	def compareAndSet[A, B](a: Ref[A], a0: A, a1: A, b: Ref[B], b0: B, b1: B): Boolean = throw new AbstractMethodError
	def compareAndSetIdentity[A <: AnyRef, B <: AnyRef](a: Ref[A], a0: A, a1: A, b: Ref[B], b0: B, b1: B): Boolean = throw new AbstractMethodError
	def withRetryTimeoutNanos(timeout: Option[Long]): TxnExecutor = throw new AbstractMethodError
	
	def isControlFlow(x: Throwable): Boolean = controlFlowTest(x)

	def withControlFlowRecognizer(pf: PartialFunction[Throwable, Boolean]): TxnExecutor = {
		copy(controlFlowTest = { x: Throwable => if (pf.isDefinedAt(x)) pf(x) else controlFlowTest(x) })
	}
	def withPostDecisionFailureHandler(handler: (Txn.Status, Throwable) => Unit): TxnExecutor = {
		copy(postDecisionFailureHandler = handler)
	}

	override def toString: String = {
		("MotTxnExecutor@" + hashCode.toHexString +
			"(retryTimeoutNanos=" + retryTimeoutNanos +
			", controlFlowTest=" +
			(if (controlFlowTest eq HaiTxnExecutor.DefaultControlFlowTest) "default" else controlFlowTest) +
			", postDecisionFailureHandler=" +
			(if (postDecisionFailureHandler eq HaiTxnExecutor.DefaultPostDecisionFailureHandler) "default" else postDecisionFailureHandler) +
			")")
	}

}
