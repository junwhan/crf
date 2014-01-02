package org.hyflow.benchmarks.skiplist

import org.hyflow.Hyflow
import org.hyflow.api._
import org.hyflow.benchmarks._
import org.hyflow.core.util.HyflowConfig
import scala.collection._
import scala.annotation.tailrec
import scala.concurrent.stm._
import scala.util.Random
import org.eintr.loglady.Logging

// TODO: implement BenchmarConfig class, which takes defaults from workload._
object SkiplistConfig {
	val MAX_LEVEL = HyflowConfig.cfg[Int]("hyflow.workload.skiplist.maxLevel")
	val PROBABILITY = HyflowConfig.cfg[Float]("hyflow.workload.skiplist.probability");
	val KEY_RANGE = HyflowConfig.cfg[Int]("hyflow.workload.skiplist.keyRange")
	val DEBUG = HyflowConfig.cfg[Boolean]("hyflow.workload.skiplist.debug")

	val OPS = HyflowConfig.cfg[Int]("hyflow.workload.ops")
}

class SkiplistBenchmark extends Benchmark {
	val name = "skiplist"

	def benchInit() {
		val slice = getLocalSlice
		for (i <- slice) {
			new SkipList[Int]("skip" + i)
		}
	}

	def benchIter() {
		val ids = randomObjIdSet(SkiplistConfig.OPS,
			() => ("skip" + rand.nextInt(totalObjects) -> rand.nextInt(SkiplistConfig.KEY_RANGE)))
		if (randomlyReadOnly)
			SkiplistOps.read(rand, ids: _*)
		else
			SkiplistOps.write(rand, ids: _*)
	}

	def benchCheck() = true
}

object SkiplistOps extends Logging {
	def read(rand: Random, ids: Tuple2[String, Int]*) {
		atomic { implicit txn =>
			for (id <- ids) {
				log.info("Operation: Contains() | id = %s", id)
				val sl = Hyflow.dir.open[SkipList[Int]](id._1.split("-")(0))
				sl.contains(id._2)
				if (SkiplistConfig.DEBUG) {
					log.debug("Trying stringRepr")
					log.info("After Contains(). | %s", sl.stringRepr)
				}
			}
		}
	}

	def write(rand: Random, ids: Tuple2[String, Int]*) {
		atomic { implicit txn =>
			val choose = rand.nextFloat()
			for (id <- ids) {
				val sl = Hyflow.dir.open[SkipList[Int]](id._1.split("-")(0))
				if (choose > 0.5) {
					log.info("Operation: Insert() | id = %s", id)
					sl.insert(id._2)
					if (SkiplistConfig.DEBUG) {
						log.debug("Trying stringRepr")
						log.info("After Insert(). | %s", sl.stringRepr)
					}
				} else {
					log.info("Operation: Delete() | id = %s", id)
					sl.delete(id._2)
					if (SkiplistConfig.DEBUG) {
						log.debug("Trying stringRepr")
						log.info("After Delete(). | %s", sl.stringRepr)
					}
				}
			}
		}
	}
}

class SkipNode[T <% Comparable[T]](val _id: String, level0: Int, value0: T, private val maxLevel: Int) extends HObj {
	val value = field(value0)
	val level = field(level0)
	val next = field(IndexedSeq.fill[String](maxLevel + 1)(null))

	// Simplified constructor
	def this(_id: String, level0: Int, value0: T) = this(_id, level0, value0, level0 + 1)

	// Getter/setter for individual element in next array
	def getNext(level: Int)(implicit txn: InTxn) = next().apply(level)
	def setNext(level: Int, newval: String)(implicit txn: InTxn) { next() = next().updated(level, newval) }
}

class SkipList[T <% Comparable[T]](val _id: String) extends HObj with Logging {
	// TODO: Oops! Node is made visible before its children if outside of a transaction!
	private val header = _id + "-header"
	private val maxLevel = SkiplistConfig.MAX_LEVEL
	new SkipNode[T](header, 0, null.asInstanceOf[T], maxLevel + 1)

	private def getNode(id: String) = Hyflow.dir.open[SkipNode[T]](id)
	private def randomLevel = {
		import scala.math
		val rlevel = (math.log(1.0 - math.random) / math.log(1.0 - SkiplistConfig.PROBABILITY)).toInt
		math.min(rlevel, SkiplistConfig.MAX_LEVEL)
	}

	def contains(value: T)(implicit txn: InTxn) = {
		// Inner function to be called for each level
		@tailrec
		def containsRec(crt: SkipNode[T], i: Int): Boolean = {
			val next = getNode(crt.getNext(i))
			if (next == null) {
				if (i == 0) false else containsRec(crt, i - 1)
			} else {
				next.value().compareTo(value) match {
					// Found the target, return
					case 0 => true
					// Haven't reached target yet, continue at same level
					case r if (r < 0) => containsRec(next, i)
					// We passed our target (r>0), continue on next level
					case r if (i > 0) => containsRec(crt, i - 1)
					// We passed our target, but there are no more levels. return
					case _ => false
				}
			}
		}

		val h = getNode(header)
		containsRec(h, h.level())
	}

	@tailrec
	private def findUpdatesRec(crt: SkipNode[T], i: Int, value: T)(implicit txn: InTxn, updates: mutable.IndexedSeq[SkipNode[T]]) {
		val next = getNode(crt.getNext(i))
		if (next == null) {
			// If there is no next value at the current level, remember current node
			// at this level and go one level down, if possible.
			updates(i) = crt
			if (i != 0)
				findUpdatesRec(crt, i - 1, value)
		} else {
			if (next.value().compareTo(value) >= 0) {
				// If we passed the target using a link from this level, remember current
				// node at this level, and go one level down, if possible. Treat equality
				// the same, as we want all links at all levels.
				updates(i) = crt
				if (i != 0) findUpdatesRec(crt, i - 1, value)
			} else {
				// Target not passed yet, continue at same level
				findUpdatesRec(next, i, value)
			}
		}
	}

	def insert(value: T)(implicit txn: InTxn) = {
		val headerObj = getNode(header)
		val oldLevel = headerObj.level()

		implicit val updates = mutable.IndexedSeq.fill[SkipNode[T]](maxLevel + 1)(null)
		findUpdatesRec(headerObj, oldLevel, value)

		val next = getNode(updates(0).getNext(0))
		if (next == null || next.value() != value) {
			val newLevel = randomLevel

			// record updates to header due to increasing the level
			if (newLevel > oldLevel) {
				for (i <- oldLevel + 1 to newLevel)
					updates(i) = headerObj
				headerObj.level() = newLevel
			}

			// insert new node
			val newId = _id + "-node-" + scala.util.Random.nextInt.toHexString
			val newNode = new SkipNode[T](newId, newLevel, value)
			for (i <- 0 to newLevel) {
				newNode.setNext(i, updates(i).getNext(i));
				updates(i).setNext(i, newId);
			}
			true
		} else
			false // Value was already in list
	}

	def delete(value: T)(implicit txn: InTxn) = {
		val headerObj = getNode(header)
		val oldLevel = headerObj.level()

		implicit val updates = mutable.IndexedSeq.fill[SkipNode[T]](maxLevel + 1)(null)
		findUpdatesRec(headerObj, oldLevel, value)

		val node = getNode(updates(0).getNext(0))
		if (node != null && node.value() == value) {
			// Remove node from list
			val nodeId = node._id
			@tailrec def updateLinksRec(i: Int) {
				if (updates(i).getNext(i) == nodeId) {
					updates(i).setNext(i, node.getNext(i))
					if (i + 1 <= oldLevel) updateLinksRec(i + 1)
				}
			}
			updateLinksRec(0)

			// De-register node
			Hyflow.dir.delete(nodeId)

			// Decrease list level if needed
			var newLevel = oldLevel
			while (newLevel > 0 && headerObj.getNext(newLevel) == null)
				newLevel -= 1
			if (newLevel != oldLevel)
				headerObj.level() = newLevel
			true
		} else
			false
	}

	def stringRepr(implicit txn: InTxn) = {
		val headerObj = getNode(header)
		val strRepr = mutable.StringBuilder.newBuilder
		var count = 0

		@tailrec def mkStringRec(node: SkipNode[T], skipheader: Boolean = true) {
			if (!skipheader) {
				strRepr.append("[")
				//strRepr.append(node._id)
				//strRepr.append(": ")
				strRepr.append(node.value().toString)
				strRepr.append("], ")
				count += 1
			} else
				strRepr.append("[[HEAD]], ")
			val next = getNode(node.getNext(0))
			if (next == null)
				strRepr.append("[[NULL]]")
			else
				mkStringRec(next, false)
		}

		mkStringRec(headerObj)
		strRepr.append(" --- len=")
		strRepr.append(count)
		strRepr.toString
	}
}

