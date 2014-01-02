package scala.concurrent.stm.motstm

import org.hyflow._
import scala.concurrent.stm.{InTxn, MaybeTxn, impl}

class OpenNestingBlock[A](above: => A) {
	def onCommit[Z](below: InTxn => Z)(implicit mt: MaybeTxn): A = {
		if (!atomic.setCommitHandler(mt, below)) {
			above
		} else {
			try {
				above
			} catch {
				case impl.AlternativeResult(x) => x.asInstanceOf[A]
			}
		}
	}
	def onAbort[Z](below: InTxn => Z)(implicit mt: MaybeTxn): A = {
		if (!atomic.setAbortHandler(mt, below)) {
			above
		} else {
			try {
				above
			} catch {
				case impl.AlternativeResult(x) => x.asInstanceOf[A]
			}
		}
	}
}

