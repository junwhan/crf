package org.hyflow.benchmarks.hashtable

import org.hyflow.api._
import org.hyflow.benchmarks._
import org.hyflow.core.directory.Tracker
import org.hyflow.core.util.HyflowConfig
import org.hyflow.Hyflow
import org.eintr.loglady.Logging
import scala.concurrent.stm._
import scala.util.Random

// Benchmark class
class HashtableBenchmark extends Benchmark {
	val name = "hashtable"
	val ops = HyflowConfig.cfg[Int]("hyflow.workload.ops")
	val buckets = HyflowConfig.cfg[Int]("hyflow.workload.hashtable.buckets")
	val keyRange = HyflowConfig.cfg[Int]("hyflow.workload.hashtable.keyRange")
	val par = true

	def benchInit() {
		val slice = getLocalSlice
		for (i <- slice) {
			new HashTable[Int, Int]("hash"+i, buckets)
		}
	}

	def benchIter() {
		val ids = randomObjIdSet(ops, () => ("hash"+rand.nextInt(totalObjects) -> rand.nextInt(keyRange)))
		if (randomlyReadOnly) {
			if (par) {
				HashtableParOps.read(rand, ids: _*)
			} else {
				HashtableSerOps.read(rand, ids: _*)
			}
		} else {
			if (par) {
				HashtableParOps.write(rand, ids: _*)
			} else {
				HashtableSerOps.write(rand, ids: _*)
			} 
		}
	}

	def benchCheck() = true
}

// Hashtable Operations (Transactions)
object HashtableParOps {
	private def getBucket2[K,V](keys: List[Tuple2[HashTable[K,V], K]]): List[HashBucket[K, V]] = {
		Hyflow.dir.openMany(keys.map { args =>
			val (ht, id2) = args 
			val bk = id2.## % ht.buckets
			ht._id + "-b" + bk
		})
	}
	
	def read(rand: Random, ids: Tuple2[String,Int]*) {
		atomic { implicit txn =>
			val hts = Hyflow.dir.openMany[HashTable[Int,Int]](ids.map(_._1.split("-")(0)).toList)
			val keys = ids.zip(hts).map { args =>
				val (id, ht) = args
				(ht, id._2)
			}
			val buckets = getBucket2(keys.toList)
			ids.zip(buckets).map { args =>
				val (id, bucket) = args
				bucket.get(id._2)
			}
		}
	}
	
	def write(rand: Random, ids: Tuple2[String,Int]*) {
		atomic { implicit txn =>
			val hts = Hyflow.dir.openMany[HashTable[Int,Int]](ids.map(_._1.split("-")(0)).toList)
			val keys = ids.zip(hts).map { args =>
				val (id, ht) = args
				(ht, id._2)
			}
			val buckets = getBucket2(keys.toList)
			ids.zip(buckets).map { args =>
				val (id, bucket) = args
				val choose = rand.nextFloat()
				if (choose > 0.5) 
					bucket.put(id._2, rand.nextInt())
				else
					bucket.remove(id._2)
			}
		}
	}
}

object HashtableSerOps {
	
	def read(rand: Random, ids: Tuple2[String,Int]*) {
		atomic { implicit txn =>
			for (id <- ids) {
				val ht = Hyflow.dir.open[HashTable[Int, Int]](id._1.split("-")(0))
				ht.get(id._2)
			}
		}
	}
	
	def write(rand: Random, ids: Tuple2[String,Int]*) {
		atomic { implicit txn => 
			for (id <- ids) {
				val ht = Hyflow.dir.open[HashTable[Int, Int]](id._1.split("-")(0))
				val choose = rand.nextFloat()
				if (choose > 0.5) 
					ht.put(id._2, rand.nextInt())
				else
					ht.remove(id._2)
			}
		}
	}
}

// Hashbucket
class HashBucket[K, V](val _id: String) extends HObj {
	// STM fields
	val content = field(Map[K, V]())

	// Transactional operations
	def get(key: K)(implicit txn: InTxn): Option[V] = content().get(key)

	def put(key: K, value: V)(implicit txn: InTxn): Option[V] = {
		val oldmap = content()
		content() = oldmap + (key -> value)
		oldmap.get(key)
	}

	def remove(key: K)(implicit txn: InTxn): Option[V] = {
		val oldmap = content()
		content() = oldmap - key
		oldmap.get(key)
	}

	def contains(key: K)(implicit txn: InTxn): Boolean = content().contains(key)
}

// Hashtable
class HashTable[K, V](val _id: String, val buckets: Int) extends HObj {
	// Initialize buckets
	for (i <- 0 to buckets)
		new HashBucket(_id + "-b" + i)

	private def getBucket(key: K): HashBucket[K, V] = {
		val bk = key.## % buckets
		Hyflow.dir.open(_id + "-b" + bk)
	}
	
	def get(key: K): Option[V] = {
		atomic { implicit txn =>
			getBucket(key).get(key)
		}
	}

	def put(key: K, value: V) = {
		atomic { implicit txn =>
			getBucket(key).put(key, value)
		}
	}

	def remove(key: K) = {
		atomic { implicit txn =>
			getBucket(key).remove(key)
		}
	}

	def contains(key: K) = {
		atomic { implicit txn =>
			getBucket(key).contains(key)
		}
	}
}