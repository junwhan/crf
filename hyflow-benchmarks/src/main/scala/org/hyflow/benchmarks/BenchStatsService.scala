package org.hyflow.benchmarks

import org.hyflow.Hyflow
import org.hyflow.api._
import org.hyflow.core.util.HyflowConfig
import org.eintr.loglady.Logging
import akka.actor._
import java.io.{FileWriter, BufferedWriter}
import java.util.Date

object BenchStatsService extends Service with Logging {

	case class AggrStats(thr: Float, commits: Int, aborts: Int) extends Message

	val name = "bench-stats"
	val actorProps = Props[BenchStatsService]
	def accepts(message: Any): Boolean = message match {
		case _: AggrStats => true
		case _ => false
	}
	
	def nodeDone(thr: Float, commits: Int, aborts: Int) {
		Hyflow.peers(0) ! AggrStats(thr, commits, aborts)
	}

}

class BenchStatsService extends Actor with Logging {

	import BenchStatsService._

	var totalThroughput = 0.0
	var totalCommits = 0
	var totalAborts = 0
	var count = 0

	def receive() = {
		case m: AggrStats =>
			totalThroughput += m.thr
			totalCommits += m.commits
			totalAborts += m.aborts
			count += 1
			if (count >= Hyflow.peers.size) {
				log.info("All peers completed. Total throughput = %f", totalThroughput)
				// Write this info to file too
				var bw: BufferedWriter = null
				try {
					bw = new BufferedWriter(
						new FileWriter("result-"+HyflowConfig.cfg[String]("hyflow.logging.hostname")+".txt", true))
					bw.write("Benchmark: %s".format(BenchApp.bench.name))
					bw.newLine()
					bw.write("Arguments: %s".format(BenchApp.arguments.toList))
					bw.newLine()
					bw.write("Ended at: %s".format(new java.util.Date))
					bw.newLine()
					bw.write("Throughput: %f".format(totalThroughput))
					bw.newLine()
					bw.write("Commits: %d".format(totalCommits))
					bw.newLine()
					bw.write("Aborts: %d".format(totalAborts))
					bw.newLine()
					bw.write("===")
					bw.newLine()
					bw.flush()
				} finally {
					if (bw != null) bw.close()
				}

			}
	}
}