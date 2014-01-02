package org.hyflow.benchmarks.rbt

import org.hyflow.benchmarks._
import org.hyflow.core.util.HyflowConfig
import org.hyflow.api._
import org.hyflow.Hyflow

import org.eintr.loglady.Logging

import scala.concurrent.stm._

class CpRBTBenchmark extends Benchmark {
	val name = "cp-rbt"
	val keyRange = HyflowConfig.cfg[Int]("hyflow.workload.keyRange")
	val ops = HyflowConfig.cfg[Int]("hyflow.workload.ops")

	def benchInit() {
		val slice = getLocalSlice
		for (i <- slice) {
			val rbt = new RBTree[Int, Int]("rbt" + i)
		}
	}

	def benchIter() {
		val ids = randomObjIdSet(ops, () => ("rbt" + rand.nextInt(totalObjects) -> rand.nextInt(keyRange)))
		if (randomlyReadOnly)
			RBTOps.read(ids: _*)
		else
			RBTOps.write(ids: _*)
	}

	def benchCheck() = true
}

object RBTOps {
	def read(ids: Tuple2[String, Int]*) {
		atomic { implicit txn =>
			var i = 0
			while (i < ids.length) {
				val id = ids(i)
				atomic { implicit txn => 
					val rbt = Hyflow.dir.open[RBTree[Int,Int]](id._1)
					rbt.get(id._2)
				}
				i += 1
			}
		}
	}

	def write(ids: Tuple2[String, Int]*) {
		atomic { implicit txn =>
			val choose = scala.math.random
			var i = 0
			while (i < ids.length) {
				val id = ids(i)
				val rbt = Hyflow.dir.open[RBTree[Int,Int]](id._1)
				if (choose > 0.5)
					rbt.put(id._2, i)
				else
					rbt.delete(id._2)
				i += 1
			}
		}
	}

}

object RBTColor extends Enumeration {
	type Color = Value
	val RED, BLACK = Value
}

// ---
// Red-Black Tree implementation adapted from 
// http://en.literateprograms.org/Red-black_tree_(Java)
// ---

class RBNode[K <% Comparable[K], V](
		val _id: String, 
		key0: K, 
		value0: V,
		color0: RBTColor.Color,
		left0: String,
		right0: String) extends HObj {
	import RBTree._

	// node fields
	val key = field(key0)
	val value = field(value0)
	val left = field[String](left0)
	val right = field[String](right0)
	val parent = field[String](null)
	val color = field[RBTColor.Color](RBTColor.RED)
	
	// Update children's parent
	// Can we avoid txn here?
	atomic { implicit txn =>
		if (left0 != null) {
			getNode0(left0).parent() = _id
		}
		if (right0 != null) {
			getNode0(right0).parent() = _id
		}
	}

	// util
	def getNode(id: String) = RBTree.getNode[K, V](id)

	// node constructor

	// node relationships
	def grandparent(implicit txn: InTxn): RBNode[K, V] = {
		val parNode = getNode(parent())
		if (parent == null) {
			error("Can not get grandparent for the root node.")
		}
		val gparNode = getNode(parNode.parent())
		if (gparNode == null) {
			error("Can not get grandparent for child of root node.")
		}
		gparNode
	}

	def sibling(implicit txn: InTxn): RBNode[K, V] = {
		val parNode = getNode(parent())
		if (parNode == null) {
			error("Root node has no sibling.")
		}
		if (_id == parNode.left())
			getNode(parNode.right())
		else
			getNode(parNode.left())
	}

	def uncle(implicit txn: InTxn): RBNode[K, V] = {
		val parNode = getNode(parent())
		if (parent == null) {
			error("Can not get uncle for the root node.")
		}
		if (parNode.parent() == null) {
			error("Can not get uncle for child of root node.")
		}
		parNode.sibling
	}
}

object RBTree {
	// constants
	val VERIFY_RBTREE = true
	val INDENT_STEP = 4
	
	def getRbnId(n: RBNode[_,_]): String = {
		if (n == null) null else n._id
	}
	def getNode[K <% Comparable[K], V](id: String) = {
		Hyflow.dir.open[RBNode[K, V]](id)
	}

	def getNode0(id: String) = {
		Hyflow.dir.open[RBNode[_, _]](id)
	}

	private def nodeColor(n: RBNode[_, _])(implicit txn: InTxn): RBTColor.Color = {
		if (n == null)
			RBTColor.BLACK
		else
			n.color()
	}

	private def verifyProperty1(n: RBNode[_, _])(implicit txn: InTxn) {
		val nc = nodeColor(n)
		if (nc != RBTColor.RED && nc != RBTColor.BLACK)
			error("Unrecognized color.")
		if (n != null) {
			verifyProperty1(getNode0(n.left()))
			verifyProperty1(getNode0(n.right()))
		}
	}

	private def verifyProperty2(root: RBNode[_, _])(implicit txn: InTxn) {
		if (nodeColor(root) != RBTColor.BLACK)
			error("Root node must be black.")
	}

	private def verifyProperty4(n: RBNode[_, _])(implicit txn: InTxn) {
		if (nodeColor(n) == RBTColor.RED) {
			// leaf (null) nodes are black, this won't be executed in that case
			if (nodeColor(getNode0(n.left())) != RBTColor.BLACK ||
				nodeColor(getNode0(n.right())) != RBTColor.BLACK ||
				nodeColor(getNode0(n.parent())) != RBTColor.BLACK)
				error("Property 4 (coloring) verification failed.")
		}
		if (n != null) {
			verifyProperty4(getNode0(n.left()))
			verifyProperty4(getNode0(n.right()))
		}
	}

	private def verifyProperty5(root: RBNode[_, _])(implicit txn: InTxn) {
		verifyProperty5Helper(root, 0, -1)
	}

	private def verifyProperty5Helper(n: RBNode[_, _],
		blackCount0: Int,
		pathBlackCount0: Int)(implicit txn: InTxn): Int = {
		var blackCount = blackCount0
		var pathBlackCount = pathBlackCount0

		if (nodeColor(n) == RBTColor.BLACK) {
			blackCount += 1
		}
		if (n == null) {
			if (pathBlackCount == -1) {
				pathBlackCount = blackCount;
			} else if (blackCount != pathBlackCount) {
				error("Property 5 (path) verification failed.")
			}
			return pathBlackCount;
		}
		pathBlackCount = verifyProperty5Helper(getNode0(n.left()), blackCount, pathBlackCount);
		pathBlackCount = verifyProperty5Helper(getNode0(n.right()), blackCount, pathBlackCount);
		return pathBlackCount;
	}

}

class RBTree[K <% Comparable[K], V](val _id: String) extends HObj with Logging {
	import RBTree._

	// root
	val root = field[String](null)

	// property verification 
	def verifyProperties(implicit txn: InTxn) {
		if (VERIFY_RBTREE) {
			verifyProperty1(getNode0(root()))
			verifyProperty2(getNode0(root()))
			// Property 3 is implicit
			verifyProperty4(getNode0(root()))
			verifyProperty5(getNode0(root()))
		}
	}
	
	// lookup operation
	private def lookupNode(key: K)(implicit txn: InTxn): RBNode[K,V] = {
	    var n = getNode[K,V](root())
	    while (n != null) {
	        val compResult = key.compareTo(n.key())
	        if (compResult == 0) {
	            return n
	        } else if (compResult < 0) {
	            n = getNode(n.left())
	        } else {
	            n = getNode(n.right())
	        }
	    }
	    n
	}
	
	def get(key: K)(implicit txn: InTxn): V = {
		//atomic { implicit txn =>
			val n = lookupNode(key)
			if (n == null) 
				null.asInstanceOf[V] 
			else 
				n.value()
		//}
	}
	
	// rotations
	private def rotateLeft(n: RBNode[K,V])(implicit txn: InTxn) {
	    val r = getNode[K,V](n.right())
	    replaceNode(n, r)
	    n.right() = r.left()
	    if (r.left() != null) {
	        getNode[K,V](r.left()).parent() = getRbnId(n)
	    }
	    r.left() = getRbnId(n)
	    n.parent() = getRbnId(r)
	}
	
	private def rotateRight(n: RBNode[K,V])(implicit txn: InTxn) {
	    val l = getNode[K,V](n.left())
	    replaceNode(n, l)
	    n.left() = l.right()
	    if (l.right() != null) {
	        getNode[K,V](l.right()).parent() = getRbnId(n)
	    }
	    l.right() = getRbnId(n)
	    n.parent() = getRbnId(l)
	}
	
	private def replaceNode(oldn: RBNode[K,V], newn: RBNode[K,V])(implicit txn: InTxn) {
	    if (oldn.parent() == null) {
	        root() = getRbnId(newn)
	    } else {
	        if (getRbnId(oldn) == getNode[K,V](oldn.parent()).left())
	            getNode[K,V](oldn.parent()).left() = getRbnId(newn)
	        else
	            getNode[K,V](oldn.parent()).right() = getRbnId(newn)
	    }
	    if (newn != null) {
	        newn.parent() = oldn.parent()
	    }
	}
	
	def put(key: K, value: V)(implicit txn: InTxn) {
		//atomic { implicit txn => 
		    val newId = _id + "-" + scala.util.Random.nextInt.toHexString
		    var newNode: RBNode[K,V] = null
		    
		    if (root() == null) {
		    	newNode = new RBNode[K,V](newId, key, value, RBTColor.RED, null, null)
		        root() = newId
		    } else {
		        var n = getNode[K,V](root())
		        while (newNode == null) {
		            val compResult = key.compareTo(n.key())
		            if (compResult == 0) {
		                n.value() = value
		                return
		            } else if (compResult < 0) {
		                if (n.left() == null) {
		                	newNode = new RBNode[K,V](newId, key, value, RBTColor.RED, null, null)
		                    n.left() = newId
		                } else {
		                    n = getNode(n.left())
		                }
		            } else {
		                if (n.right() == null) {
		                	newNode = new RBNode[K,V](newId, key, value, RBTColor.RED, null, null)
		                    n.right() = newId
		                } else {
		                    n = getNode(n.right())
		                }
		            }
		        }
		        newNode.parent() = n._id
		    }
		    // Make sure properties are still satisfied
		    insertCase1(newNode)
		    //verifyProperties()
		//}
	    
	    // Post-insertion procedures (coloring & rebalancing)
	    def insertCase1(n: RBNode[K,V])(implicit txn: InTxn) {
	    	if (n.parent() == null)
	    		n.color() = RBTColor.BLACK
	    	else
	    		insertCase2(n)
	    }
	    
	    def insertCase2(n: RBNode[K,V])(implicit txn: InTxn) {
	    	if (nodeColor(getNode[K,V](n.parent())) == RBTColor.BLACK)
	    		return; // Tree is still valid
	    	else
	    		insertCase3(n)
	    }
	    
	    def insertCase3(n: RBNode[K,V])(implicit txn: InTxn) {
	    	if (nodeColor(n.uncle) == RBTColor.RED) {
		        getNode[K,V](n.parent()).color() = RBTColor.BLACK
		        n.uncle.color() = RBTColor.BLACK
		        n.grandparent.color() = RBTColor.RED
		        insertCase1(n.grandparent)
		    } else {
		        insertCase4(n)
		    }
	    }
	    
	    def insertCase4(n: RBNode[K,V])(implicit txn: InTxn) {
	    	val n2 = if (getRbnId(n) == getNode[K,V](n.parent()).right() && 
	    			n.parent() == n.grandparent.left()) {
		        rotateLeft(getNode(n.parent()))
		        getNode[K,V](n.left())
		    } else if (getRbnId(n) == getNode[K,V](n.parent()).left() && 
		    		n.parent() == n.grandparent.right()) {
		        rotateRight(getNode(n.parent()))
		        getNode[K,V](n.right())
		    } else n
		    insertCase5(n2)
	    }
	    
	    def insertCase5(n: RBNode[K,V])(implicit txn: InTxn) {
	    	getNode[K,V](n.parent()).color() = RBTColor.BLACK
		    n.grandparent.color() = RBTColor.RED
		    if (getRbnId(n) == getNode[K,V](n.parent()).left() && n.parent() == n.grandparent.left()) {
		        rotateRight(n.grandparent)
		    } else {
		        if (! (getRbnId(n) == getNode[K,V](n.parent()).right() && 
		        		n.parent() == n.grandparent.right()))
		        	error("RBT insertCase5 assertion failed.")
		        rotateLeft(n.grandparent)
		    }
	    }
	}
	
	def delete(key: K)(implicit txn: InTxn) {
		
		// Sub-functions
	    def maximumNode(n: RBNode[K,V])(implicit txn: InTxn): RBNode[K,V] = {
			if (n == null)
				error("Assertion failed: n != null in maximumNode")
			var maxn = n
		    while (maxn.right() != null) {
		        maxn = getNode(maxn.right())
		    }
		    maxn
		}
	    
	    def deleteCase1(n: RBNode[K,V])(implicit txn: InTxn) {
		    if (null != n.parent())
		        deleteCase2(n)
		}
	    
	    def deleteCase2(n: RBNode[K,V])(implicit txn: InTxn) {
		    if (nodeColor(n.sibling) == RBTColor.RED) {
		        getNode[K,V](n.parent()).color() = RBTColor.RED
		        n.sibling.color() = RBTColor.BLACK
		        if (getRbnId(n) == getNode[K,V](n.parent()).left())
		            rotateLeft(getNode(n.parent()))
		        else
		            rotateRight(getNode(n.parent()))
		    }
		    deleteCase3(n)
		}
	    
	    def deleteCase3(n: RBNode[K,V])(implicit txn: InTxn) {
		    if (nodeColor(getNode[K,V](n.parent())) == RBTColor.BLACK &&
		    		nodeColor(n.sibling) == RBTColor.BLACK &&
		    		nodeColor(getNode[K,V](n.sibling.left())) == RBTColor.BLACK &&
		    		nodeColor(getNode[K,V](n.sibling.right())) == RBTColor.BLACK) {
		        n.sibling.color() = RBTColor.RED
		        deleteCase1(getNode(n.parent()))
		    }
		    else
		        deleteCase4(n)
		}
	    
	    def deleteCase4(n: RBNode[K,V])(implicit txn: InTxn) {
	    	if (nodeColor(getNode[K,V](n.parent())) == RBTColor.RED &&
	    			nodeColor(n.sibling) == RBTColor.BLACK &&
	    			nodeColor(getNode[K,V](n.sibling.left())) == RBTColor.BLACK &&
	    			nodeColor(getNode[K,V](n.sibling.right())) == RBTColor.BLACK) {
		        n.sibling.color() = RBTColor.RED
		        getNode[K,V](n.parent()).color() = RBTColor.BLACK
		    }
		    else
		        deleteCase5(n);
		}
	    
	    def deleteCase5(n: RBNode[K,V])(implicit txn: InTxn) {
	    	if (getRbnId(n) == getNode[K,V](n.parent()).left() &&
		        nodeColor(n.sibling) == RBTColor.BLACK &&
		        nodeColor(getNode[K,V](n.sibling.left())) == RBTColor.RED &&
		        nodeColor(getNode[K,V](n.sibling.right())) == RBTColor.BLACK)
		    {
		        n.sibling.color() = RBTColor.RED;
		        getNode[K,V](n.sibling.left()).color() = RBTColor.BLACK;
		        rotateRight(n.sibling)
		    }
		    else if (getRbnId(n) == getNode[K,V](n.parent()).right() &&
		             nodeColor(n.sibling) == RBTColor.BLACK &&
		             nodeColor(getNode[K,V](n.sibling.right())) == RBTColor.RED &&
		             nodeColor(getNode[K,V](n.sibling.left())) == RBTColor.BLACK)
		    {
		        n.sibling.color() = RBTColor.RED
		        getNode[K,V](n.sibling.right()).color() = RBTColor.BLACK
		        rotateLeft(n.sibling)
		    }
		    deleteCase6(n);
	    }
	    
	    def deleteCase6(n: RBNode[K,V])(implicit txn: InTxn) {
	    	n.sibling.color() = nodeColor(getNode[K,V](n.parent()))
		    getNode[K,V](n.parent()).color() = RBTColor.BLACK
		    if (getRbnId(n) == getNode[K,V](n.parent()).left()) {
		    	if (nodeColor(getNode[K,V](n.sibling.right())) != RBTColor.RED)
		    		error("Assertion failed deleteCase6: nodeColor(n.sibling().right) == Color.RED")
		        getNode[K,V](n.sibling.right()).color() = RBTColor.BLACK
		        rotateLeft(getNode(n.parent()))
		    }
		    else
		    {
		    	if (nodeColor(getNode[K,V](n.sibling.left())) != RBTColor.RED)
		    		error("Assertion failed deleteCase6: nodeColor(n.sibling().left) == Color.RED")
		        getNode[K,V](n.sibling.left()).color() = RBTColor.BLACK
		        rotateRight(getNode[K,V](n.parent()))
		    }
	    }
		
		//atomic { implicit txn => 
		    var n = lookupNode(key)
		    if (n == null)
		        return;  // Key not found, do nothing
		    if (n.left() != null && n.right() != null) {
		        // Copy key/value from predecessor and then delete it instead
		        val pred = maximumNode(getNode[K,V](n.left()))
		        n.key()   = pred.key()
		        n.value() = pred.value()
		        n = pred
		    }
		
		    //assert n.left == null || n.right == null;
		    val child = getNode[K,V](if (n.right() == null) n.left() else n.right())
		    if (nodeColor(n) == RBTColor.BLACK) {
		        n.color() = nodeColor(child)
		        deleteCase1(n)
		    }
		    replaceNode(n, child)
		    
		    if (nodeColor(getNode[K,V](root())) == RBTColor.RED) {
		        getNode[K,V](root()).color() = RBTColor.BLACK
		    }
		
		    //verifyProperties()
		//}
	}
	
	def print() {
		atomic { implicit txn => 
			log.debug("Printing tree:")
			printHelper(getNode(root()), 0)
		}
	    
	    def printHelper(n: RBNode[K,V], indent: Int)(implicit txn: InTxn) {
		    if (n == null) {
		        log.debug("<empty tree>")
		        return
		    }
		    if (n.right != null) {
		        printHelper(getNode(n.right()), indent + INDENT_STEP)
		    }
		    val sb = new StringBuilder()
		    sb.append(" " * indent)
		    if (n.color() == RBTColor.BLACK)
		        sb.append(n.key())
		    else
		        sb.append("<" + n.key() + ">")
		    log.debug(sb.toString)
		    if (n.left != null) {
		        printHelper(getNode(n.left()), indent + INDENT_STEP)
		    }
		}
	}


}