package org.hyflow.benchmarks.linkedlist

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
object LinkedListConfig {
	val KEY_RANGE = HyflowConfig.cfg[Int]("hyflow.workload.skiplist.keyRange")
	val OPS = HyflowConfig.cfg[Int]("hyflow.workload.ops")
}

class CpLinkedListBenchmark extends Benchmark {
	val name = "cp-linkedlist"

	def benchInit() {
		val slice = getLocalSlice
		for (i <- slice) {
			new LinkedList[Int,Int]("link" + i)
		}
	}

	def benchIter() {
		val ids = randomObjIdSet(LinkedListConfig.OPS,
			() => ("link" + rand.nextInt(totalObjects) -> rand.nextInt(LinkedListConfig.KEY_RANGE)))
		if (randomlyReadOnly)
			LinkedListOps.read(rand, ids: _*)
		else
			LinkedListOps.write(rand, ids: _*)
	}

	def benchCheck() = true
}

object LinkedListOps extends Logging {
	def read(rand: Random, ids: Tuple2[String, Int]*) {
		atomic { implicit txn =>
			var i = 0
			while (i < ids.length) {
				val id = ids(i)
				log.info("Operation: Contains() | id = %s", id)
				val ll = Hyflow.dir.open[LinkedList[Int,Int]](id._1.split("-")(0))
				ll.get(id._2)
				i+=1
			}
		}
	}

	def write(rand: Random, ids: Tuple2[String, Int]*) {
		atomic { implicit txn =>
			val choose = rand.nextFloat()
			var i = 0
			while (i < ids.length) {
				val id = ids(i)
				val ll = Hyflow.dir.open[LinkedList[Int,Int]](id._1.split("-")(0))
				if (choose > 0.5) {
					log.info("Operation: Insert() | id = %s", id)
					ll.set(id._2, 10)
				} else {
					log.info("Operation: Delete() | id = %s", id)
					ll.delete(id._2)
				}
				i+=1
			}
		}
	}
}

class LinkedNode[K <% Comparable[K], V](val _id: String, key0: K, value0: V, next0: String) extends HObj {
	val key = field(key0)
	val value = field(value0)
	val next = field(next0)
	
	// Simplified constructor
	def this(_id: String, key0: K, value0: V) = this(_id, key0, value0, null)
}

class LinkedList[K <% Comparable[K], V](val _id: String) extends HObj with Logging {
	val header = field[String](null)

	private def getNode(id: String) = Hyflow.dir.open[LinkedNode[K, V]](id)

	def get(key: K)(implicit txn: InTxn): V = {
		@tailrec
		def getHelper(node: LinkedNode[K,V]): V = {
			if (node == null)
				return null.asInstanceOf[V]
			val cmp = key.compareTo(node.key())
			if (cmp == 0)
				node.value()
			else if (cmp > 0)
				getHelper(getNode(node.next()))
			else
				null.asInstanceOf[V]
		}
		
		if (header() == null)
			return null.asInstanceOf[V]
		else {
			getHelper(getNode(header()))
		}
		
	}

	def set(key: K, value: V)(implicit txn: InTxn): V = {
		@tailrec
		def setHelper(prev: LinkedNode[K, V], node: LinkedNode[K, V]): V = {
			if (node == null) {
				val newid = _id + "-" + scala.util.Random.nextInt.toHexString
				new LinkedNode[K,V](newid, key, value)
				if (prev == null)
					header() = newid
				else
					prev.next() = newid
				return null.asInstanceOf[V]
			}
			
			val cmp = key.compareTo(node.key())
			if (cmp == 0) {
				val res = node.value()
				node.value() = value
				res
			} else if (cmp > 0) {
				setHelper(node, getNode(node.next()))
			} else {
				// create new node
				val newid = _id + "-" + scala.util.Random.nextInt.toHexString
				new LinkedNode[K,V](newid, key, value, node._id)
				// update prev node
				if (prev == null)
					header() = newid
				else
					prev.next() = newid
				// no old value
				null.asInstanceOf[V]
			}
		}
		
		if (header() == null) {
			val newid = _id + "-" + scala.util.Random.nextInt.toHexString
			header() = newid
			new LinkedNode[K,V](newid, key, value)
			null.asInstanceOf[V]
		} else {
			setHelper(null, getNode(header()))
		}
	}

	def delete(key: K)(implicit txn: InTxn): V = {
		@tailrec
		def deleteHelper(prev: LinkedNode[K,V], node: LinkedNode[K,V]): V = {
			if (node == null)
				return null.asInstanceOf[V]
			val cmp = key.compareTo(node.key())
			if (cmp == 0) {
				val res = node.value()
				if (prev == null)
					header() = node.next()
				else
					prev.next() = node.next()
				res
			} else if (cmp > 1) {
				deleteHelper(node, getNode(node.next()))
			} else {
				null.asInstanceOf[V]
			}
		}
		
		if (header() == null) {
			null.asInstanceOf[V]
		} else {
			deleteHelper(null, getNode(header()))
		}
	}

}

