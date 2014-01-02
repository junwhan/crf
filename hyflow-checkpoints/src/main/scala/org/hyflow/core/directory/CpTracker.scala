package org.hyflow.core.directory

import org.hyflow.api._
import org.hyflow.core._
import scala.concurrent.stm._
//import org.apache.commons.javaflow.Continuation
import sun.misc.Continuation
import util.HyflowConfig

class CpTracker extends Tracker {
	override def open[T <: HObj](id: String)(implicit mt: MaybeTxn): T = {
		
		val prob = util.HyflowConfig.cfg[Int]("hyflow.haistm.checkpointProb")
		if (scala.util.Random.nextInt(100) < prob) {
			log.debug("Suspending before opening object.  | id = %s", id)
			// trigger checkpoint
			val cont = new Continuation()
			CheckpointStatus.set(CheckpointInterim())
			cont.save()
			
			log.debug("Returned from suspension for opening object.  | id = %s", id)
		} else {
			log.debug("Skipping checkpoint.")
		}
		// open as usual
		try {
			super.open(id)
		} catch {
			case CpRollbackError =>
				log.debug("Failed to open object. | id = %s", id)
				val cont = new Continuation()
				CheckpointStatus.set(CheckpointFailure())
				cont.save()
				
				log.error("This should never be reached")
				null.asInstanceOf[T]
		}
	}
}