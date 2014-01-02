package org.hyflow.api

import scala.concurrent.stm._


object HyflowExecutor {
  @volatile private var _default: HyflowExecutor = scala.concurrent.stm.motstm.MotSTM

  def defaultAtomic: HyflowExecutor = _default

  def transformDefault(f: HyflowExecutor => HyflowExecutor) {
    synchronized { _default = f(_default) }
  }

  val DefaultPostDecisionExceptionHandler = { (status: Txn.Status, x: Throwable) =>
    throw x
  }
}

// Hyflow additions to ScalaSTM
trait HyflowExecutor extends TxnExecutor {
	def open[Z](block: InTxn => Z)(implicit mt: MaybeTxn): Z
	def config[Z](block: InTxn => Z)(implicit mt: MaybeTxn): Z
	def setCommitHandler[Z](mt: MaybeTxn, block: InTxn => Z): Boolean
	def setAbortHandler[Z](mt: MaybeTxn, block: InTxn => Z): Boolean
}