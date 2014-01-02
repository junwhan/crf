package scala.concurrent.stm.motstm

import scala.concurrent.stm.InTxn
import java.util.concurrent._
import java.util.concurrent.atomic._
import org.eintr.loglady.Logging

object MotStats extends Logging {
	val counters = new ConcurrentHashMap[String, MotStats]()
	def apply(block: InTxn => Any): MotStats = {
		val bcn = block.getClass.getName
		val old = counters.get(bcn)
		if (old == null) {
			val repl = new MotStats(bcn)
			val old = counters.putIfAbsent(bcn, repl)
			if (old != null) old else repl
		} else {
			old
		}
	}
	
	def logAll() {
		import scala.collection.JavaConversions.asSet
		for (entry <- asSet(counters.keySet)) {
			log.info("%s -> %s", entry, counters.get(entry))
		}
	}
}

class MotStats(val blockClassName: String) extends Logging {
	val aborts = new AtomicLong(0)
	val commits = new AtomicLong(0)
	val pessimistic = new AtomicLong(0)
	val permanentPessimistic = new AtomicBoolean(false)
	
	def countAbort() {
		aborts.incrementAndGet()
	}
	
	def countCommit() {
		commits.incrementAndGet()
	}
	
	def countPessimistic() {
		log.debug("Block executed in pessimistic mode. | block = %s | stats = %s", blockClassName, this)
		pessimistic.incrementAndGet()
	}
	
	// TODO: turn into some kind of a moving average, to revert back to 
	// optimistic mode when contention has lowered
	def isPessimisticRequired(): Boolean = {
		val c = commits.get
		if (permanentPessimistic.get) {
			true
		// TODO: make parameters tunable
		} else if (c > 5 && pessimistic.get > 0.75 * c) {
			log.info("Entering permanent pessimistic mode. | block = %s", blockClassName)
			permanentPessimistic.set(true)
			true
		} else false
	}
	
	override def toString = "MotStats(commits=%d, aborts=%d, pessimistic=%d)".format(
			commits.get, 
			aborts.get, 
			pessimistic.get
		)
}