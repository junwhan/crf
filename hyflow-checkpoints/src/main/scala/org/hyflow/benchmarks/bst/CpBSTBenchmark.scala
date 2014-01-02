package org.hyflow.benchmarks.bst

import org.hyflow.Hyflow
import org.hyflow.api._
import org.hyflow.core.util.HyflowConfig
import org.hyflow.benchmarks.Benchmark
import scala.concurrent.stm._
import scala.annotation.tailrec
import org.eintr.loglady.Logging


class CpBSTBenchmark extends Benchmark {
	val name = "cp-bst"
	val keyRange = HyflowConfig.cfg[Int]("hyflow.workload.keyRange")
	val ops = HyflowConfig.cfg[Int]("hyflow.workload.ops")
	
	def benchInit() {
		val slice = getLocalSlice
		for (i <- slice) {
			val bst = new CpBSTHandler[Int]("bst"+i)
			bst.createTree(-1)
		}
	}

	def benchIter() {
		val ids = randomObjIdSet(ops, () => ("bst"+rand.nextInt(totalObjects) -> rand.nextInt(keyRange)))
		if (randomlyReadOnly)
			CpBSTOps.read(ids: _*)
		else
			CpBSTOps.write(ids: _*)
	}

	def benchCheck() = true
}

object CpBSTOps {
	def read(ids: Tuple2[String,Int]*) {
		atomic { implicit txn =>
			var i = 0
			while (i < ids.length) {
				val id = ids(i)
				val bst = new CpBSTHandler[Int](id._1)
				bst.find(id._2)
				i += 1
			}
		}
	}
	
	def write(ids: Tuple2[String,Int]*) {
		atomic { implicit txn =>
			val choose = scala.math.random
			var i = 0
			while (i < ids.length) {
				val id = ids(i)
				val bst = new CpBSTHandler[Int](id._1)
				if (choose > 0.5)
					bst.add(id._2)
				else
					bst.delete(id._2)
				i += 1
			}
		}
	}
	
}

class CpBSTHandler[T <% Comparable[T]](val _id: String) extends Logging {
	def header = _id + "-head"
	def getNode(id: String)(implicit txn: InTxn) = Hyflow.dir.open[BSTNode[T]](id)(txn)
	def createTree(noval: T) {
		new BSTNode[T](header, noval)

	}

	def add(value: T)(implicit txn: InTxn): Boolean = {
		var next = header
		var prev: String = null
		var right: Boolean = true
		do {
			val node = getNode(next)
			val cmp = value.compareTo(node.value())
			if (cmp == 0) {
				return false
			} else if (cmp > 0) {
				prev = next
				next = node.right()
				right = true
			} else {
				prev = next
				next = node.left()
				right = false
			}
		} while (next != null)

		val prevNode = getNode(prev)
		val newId = _id + "-" + scala.util.Random.nextInt.toHexString
		new BSTNode[T](newId, value)
		if (right)
			prevNode.right() = newId
		else
			prevNode.left() = newId
		true
	}
	
	def delete(value: T)(implicit txn: InTxn): Boolean = {
		var deleted = false
		
		def getRightMost(rootid: String): BSTNode[T] = {
			var crtid = rootid
			var crt = getNode(crtid)
			while (crt.right() != null) {
				crtid = crt.right()
				crt = getNode(crtid)
			}
			crt
		}
		
		def getLeftRepl(rootid: String): String = {
			val root = getNode(rootid)
			if (root.right() != null) {
				val repl = getLeftRepl(root.right())
				if (root.right() != repl)
					root.right() = repl
				rootid
			} else
				root.left()
		}
		
		def delete0(rootid: String, value: T): String = {
			if (rootid == null)
				return null
			
			val root = getNode(rootid)
			val cmp = value.compareTo(root.value())
			if (cmp < 0) {
				val repl = delete0(root.left(), value)
				if (root.left() != repl)
					root.left() = repl
				rootid
			} else if (cmp > 0) {
				val repl = delete0(root.right(), value)
				if (root.right() != repl)
					root.right() = repl
				rootid
			} else {
				// found it
				// TODO: do scala closures work with JavaFlow?
				deleted = true
				
				if (root.left() == null) {
					root.right()
				} else if (root.right() == null) {
					root.left()
				} else {
					// Swap data from rightmost node in left subtree
					root.value() = getRightMost(root.left()).value()
					// Replace left node
					val leftRepl = getLeftRepl(root.left())
					if (root.left() != leftRepl)
						root.left() = leftRepl
					rootid
				}
			}
		}
		
		val headNode = getNode(header)
		val repl = delete0(headNode.right(), value)
		if (headNode.right() != repl)
			headNode.right() = repl
		deleted
	}
	
	def find(value: T)(implicit txn: InTxn): Boolean = {
		var next = header
		var prev: String = null
		do {
			val node = getNode(next)
			val cmp = value.compareTo(node.value())
			if (cmp == 0) {
				return true
			} else if (cmp > 0) {
				prev = next
				next = node.right()
			} else {
				prev = next
				next = node.left()
			}
		} while (next != null)
		false
	}
}

class BSTNode[T <% Comparable[T]](val _id: String, value0: T) extends HObj {
	val value = field(value0)
	val left = field[String](null)
	val right = field[String](null)
}