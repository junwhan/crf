package org.hyflow.benchmarks

import scala.collection.mutable
import org.hyflow.Hyflow
import org.hyflow.core.util.HyflowConfig
import org.eintr.loglady.Logging
import scala.util.Random

abstract class Benchmark extends Logging {

	val rand: Random = new Random(##)

	val name: String
	protected def benchInit(): Unit
	// must be reentrant
	protected def benchIter(): Unit

	def benchCheck(): Boolean

	protected def randomObjId: String = "id-" + rand.nextLong

	val injectorThreads = HyflowConfig.cfg[Int]("hyflow.benchmark.injectorThreads")
	val testTimeLimit = HyflowConfig.cfg[Int]("hyflow.benchmark.testTime")
	val warmupTimeLimit = HyflowConfig.cfg[Int]("hyflow.benchmark.warmupTime")
	val totalObjects = HyflowConfig.cfg[Int]("hyflow.workload.objects")
	val readOnlyRatio = HyflowConfig.cfg[Float]("hyflow.workload.readOnlyRatio") * 0.01

	protected def randomlyReadOnly: Boolean = rand.nextFloat() < readOnlyRatio
	protected def randomObjIdSet(count: Int) = randomObjIdSet[String](count, randomObjId _)
	protected def randomObjIdSet[T: Manifest](count: Int, giveRandId: () => T): Array[T] = {
		val idset = mutable.Set[T]()
		while (idset.size < count)
			idset += giveRandId()
		idset.toArray
	}
	protected def getLocalSlice: Range =
		Range(Hyflow.peers.indexOf(Hyflow.mainActor), totalObjects, Hyflow.peers.size)

	//
	var warmupWallTimeLimit: Long = 0
	var testWallTimeLimit: Long = 0

	def runBenchmark() {

		// Perform per-bench initialization
		val beforeInitTime = System.currentTimeMillis
		benchInit()

		// Create injector threads
		val node = HyflowConfig.cfg[Int]("id")
		val threads = for (i <- 0 until injectorThreads) yield new InjectorThread(node, i)

		// Wait for all nodes to reach this point
		Hyflow.barrier("init-done")

		val afterInitTime = System.currentTimeMillis
		warmupWallTimeLimit = afterInitTime + warmupTimeLimit * 1000
		testWallTimeLimit = warmupWallTimeLimit + testTimeLimit * 1000

		// Start threads
		threads.foreach(_.start())

		// Wait for threads to complete
		threads.foreach(_.join())
		val afterFinishedTime = System.currentTimeMillis

		// Gather results
		val avgtime = 1.0f * threads.map(x => x.doneTime - x.firstTime).sum / injectorThreads
		val totalops = threads.map(_.opsPerformed).sum
		val throughput = 1000.0f * totalops / avgtime
		val timeEachOp = threads.foldLeft(List[Long]())(_ ++ _.timeEachOp)

		// Print statistics 
		// TODO: also print to file
		// TODO: timeEachOp histogram
		log.info("Node ops performed. | nodeOps = %d", totalops)
		log.info("Node throughput (ops/second). | node-thr = %f", throughput)

		BenchStatsService.nodeDone(throughput, Hyflow._topCommits, Hyflow._topAborts)

		// Consistency check
		if (Hyflow.peers(0) == Hyflow.mainActor) {
			benchCheck()
		}

		Hyflow.done()
	}

	private class InjectorThread(node: Int, id: Int) extends Thread("bench-n%d-t%d".format(node, id)) with Logging {
		var opsPerformed = 0
		val timeEachOp = mutable.ListBuffer[Long]()
		val rand: Random = new Random(##)
		var firstTime, doneTime: Long = 0

		def benchStage(warmup: Boolean) {
			val wallTimeLimit = if (warmup) warmupWallTimeLimit else testWallTimeLimit
			var lastTime = System.currentTimeMillis
			firstTime = lastTime
			opsPerformed = 0

			while (lastTime < wallTimeLimit) {
				try {
					benchIter()
				} catch {
					case e: Exception =>
						log.error(e, "Caught Exception in benchmark iteration.")
						//throw e
				}

				// Track throughput
				val newTime = System.currentTimeMillis
				val duration = newTime - lastTime
				timeEachOp += duration
				lastTime = newTime
				opsPerformed += 1
			}

			doneTime = lastTime
		}

		override def run() {
			log.info("Starting Warmup.")
			benchStage(true)
			log.info("Warmup complete. Measuring throughput.")
			benchStage(false)
			log.info("Benchmark done.")
		}
	}
}

