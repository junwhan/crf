package scala.concurrent.stm
package motstm


import Txn._
import org.hyflow.core.util.HyflowConfig
import skel.RollbackError

object MotUtil {
	
	
	// Retry cause
	def isExplicitRetry(status: Status): Boolean = isExplicitRetry(status.asInstanceOf[RolledBack].cause)
	def isExplicitRetry(cause: RollbackCause): Boolean = cause.isInstanceOf[ExplicitRetryCause]
	
	// Exceptional termination
	def rethrowFromStatus(status: Status) {
		status match {
			case rb: RolledBack => {
				rb.cause match {
					case UncaughtExceptionCause(x) => throw x
					case _: TransientRollbackCause => throw RollbackError
				}
			}
			case _ =>
		}
	}
	
	// Configuration
	object MotCfg {
		val CONDITIONAL_SYNC = HyflowConfig.cfg[Boolean]("hyflow.motstm.conditionalSync")
		val CLOSED_NESTING = HyflowConfig.cfg[Boolean]("hyflow.motstm.closedNesting")
		val ALWAYS_PESSIMISTIC = HyflowConfig.cfg[Boolean]("hyflow.motstm.alwaysPessimistic")
		val ALLOW_PESSIMISTIC = HyflowConfig.cfg[Boolean]("hyflow.motstm.allowPessimistic")
	}
}