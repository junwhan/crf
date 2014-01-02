package org.hyflow.core

import scala.util.control.ControlThrowable
import org.eintr.loglady.Logging

object CheckpointStatus extends ThreadLocal[CheckpointStatus] with Logging {
	override def initialValue() = CheckpointVoid()
	override def set(newval: CheckpointStatus) = {
		log.debug("Checkpoint status set to: %s.", newval)
		super.set(newval)
	}
}

sealed abstract class CheckpointStatus
case class CheckpointVoid extends CheckpointStatus
case class CheckpointInterim extends CheckpointStatus
case class CheckpointFailure extends CheckpointStatus
case class CheckpointSuccess(val result: Any) extends CheckpointStatus
case class CheckpointPermFail(val e: Exception) extends CheckpointStatus

object CpRollbackError extends Error with ControlThrowable {
  override def fillInStackTrace(): Throwable = this
}
