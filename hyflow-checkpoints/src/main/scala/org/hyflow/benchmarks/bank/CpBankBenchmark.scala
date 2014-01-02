package org.hyflow.benchmarks.bank

import org.hyflow.api._
import org.hyflow.Hyflow
import org.hyflow.benchmarks._
import scala.concurrent.stm._
import org.eintr.loglady.Logging
import org.hyflow.core.util.HyflowConfig

class CpBankBenchmark extends Benchmark with Logging {
	
	val name = "cp-bank"
	
	final val INITIAL_AMOUNT = 100
	val ops = HyflowConfig.cfg[Int]("hyflow.workload.ops")
	
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
			BankAccountOps.balanceCheck(ids: _*)
		} else {
			val ids = randomObjIdSet(2)
			BankAccountOps.transfer(ids(0), ids(1), rand.nextInt(20))
		}
	}
	
	def benchCheck(): Boolean = {
		val total = BankAccountOps.balanceCheck((0 until totalObjects).map("account-" + _): _*)
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

object BankAccountOps extends Logging {
	def deposit(id: String, amount: Int) {
		atomic { implicit txn =>
			val acc = Hyflow.dir.open[BankAccount](id)
			log.debug("after open")
			val oldbal = acc.balance()
			val newbal = oldbal + amount
			acc.balance() = newbal
			log.debug("deposit(id: %s, amount: %d) { balance: %d -> %d }", 
					id, amount, oldbal, newbal)
		}
	}
	
	def withdraw(id: String, amount: Int) {
		atomic { implicit txn =>
			val acc = Hyflow.dir.open[BankAccount](id)
			log.debug("After open")
			val oldbal = acc.balance()
			val newbal = oldbal - amount
			acc.balance() = newbal
			log.debug("withdraw(id: %s, amount: %d) { balance: %d -> %d }", 
					id, amount, oldbal, newbal)
		}
	} 
	
	def transfer(id1: String, id2: String, amount: Int) {
		atomic { implicit txn =>
			log.debug("Begin transfer")
			withdraw(id1, amount)
			deposit(id2, amount)
			log.debug("End transfer")
		}
	}
	
	def balanceCheck(ids: String*): Int = {
		atomic { implicit txn =>
			val len = ids.length
			val amounts = new Array[Int](len)
			var i = 0
			while (i < len) {
				val acc = Hyflow.dir.open[BankAccount](ids(i))
				log.debug("after open")
				amounts(i) = acc.balance()
				i += 1
			}
			amounts.sum
		}
	}
}

class BankAccount(val _id: String, val initial: Int) extends HObj with Logging {
	def this(_id: String) = this(_id, 100)
	val balance = field(initial)
}

