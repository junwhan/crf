package org.hyflow.benchmarks.bank

import org.hyflow.api._
import org.hyflow.Hyflow
import org.hyflow.benchmarks._
import scala.concurrent.stm._
import org.eintr.loglady.Logging
import org.hyflow.core.util.HyflowConfig

class BankBenchmark extends Benchmark with Logging {
	
	val name = "bank"
	
	final val INITIAL_AMOUNT = 100
	val ops = HyflowConfig.cfg[Int]("hyflow.workload.ops")
	val par = true
	
	def benchInit() {
		// Create bank accounts
		val slice = getLocalSlice
		log.debug("Initializing Bank Benchmark. | slice = %s", slice)
		val accounts = for (i <- slice) yield
			new BankAccount("account-"+i, INITIAL_AMOUNT)
		for (a <- accounts)
			log.trace("Post-init. | account = %s | account._owner = %s", a, a._owner)
		
	}
	
	def benchIter() {
		if (randomlyReadOnly) {
			val ids = randomObjIdSet(ops)
			if (par) {
				BankParOps.balanceCheck(ids: _*)
			} else {
				BankSerOps.balanceCheck(ids: _*)
			}
		} else {
			val ids = randomObjIdSet(2)
			if (par) {
				BankParOps.transfer(ids(0), ids(1), rand.nextInt(20))
			} else {
				BankSerOps.transfer(ids(0), ids(1), rand.nextInt(20))
			}
		}
	}
	
	def benchCheck(): Boolean = {
		val total = BankParOps.balanceCheck((0 until totalObjects).map("account-" + _): _*)
		if (total == INITIAL_AMOUNT * totalObjects) { 
			log.debug("Consistency check PASSED. | INITIAL_AMOUNT = %d | totalObjects = %d | total = %d",
					INITIAL_AMOUNT, totalObjects, total)
			true
		} else {
			log.error("Consistency check FAILED. | INITIAL_AMOUNT = %d | totalObjects = %d | total = %d",
					INITIAL_AMOUNT, totalObjects, total)
			false
		}
	}
	
	override def randomObjId: String = {
		"account-"+rand.nextInt(totalObjects)
	}
}

object BankParOps {
	def transfer(id1: String, id2: String, amount: Int) {
		atomic { implicit txn => 
			val accs = Hyflow.dir.openMany[BankAccount](List(id1, id2))
			accs(0).balance() = accs(0).balance() - amount
			accs(1).balance() = accs(1).balance() + amount
		}
	}
	
	def balanceCheck(ids: String*): Int = {
		atomic { implicit txn =>
			val accs = Hyflow.dir.openMany[BankAccount](ids.toList)
			val amounts = accs.map(_.balance())
			amounts.sum
		}
	}
	
}

object BankSerOps {
	def deposit(id: String, amount: Int) {
		atomic { implicit txn =>
			val acc = Hyflow.dir.open[BankAccount](id)
			acc.balance() = acc.balance() + amount
		}
	}
	
	def withdraw(id: String, amount: Int) {
		atomic { implicit txn =>
			val acc = Hyflow.dir.open[BankAccount](id)
			acc.balance() = acc.balance() - amount
		}
	} 
	
	def transfer(id1: String, id2: String, amount: Int) {
		atomic { implicit txn =>
			withdraw(id1, amount)
			deposit(id2, amount)
		}
	}
	
	def balanceCheck(ids: String*): Int = {
		atomic { implicit txn =>
			val amounts = for (id <- ids) yield {
				val acc = Hyflow.dir.open[BankAccount](id)
				acc.balance()
			}
			amounts.sum
		}
	}
}

class BankAccount(val _id: String, val initial: Int) extends HObj with Logging {
	def this(_id: String) = this(_id, 100)
	val balance = field(initial)
}

