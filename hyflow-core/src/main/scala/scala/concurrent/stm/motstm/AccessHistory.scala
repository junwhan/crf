package scala.concurrent.stm.motstm
import org.hyflow.api._
import scala.collection.mutable
import scala.annotation.tailrec
import scala.concurrent.stm._

//TODO: how much is TFA specific, and how much can we abstract out?
//TODO: do we want lists or sets?
object AccessHistory {
	trait ReadSet {
		def rsCount: Int
		def rsRecRead(handle: Handle[_])
		def rsHandleSet: Set[Handle[_]]
		def rsMergeFrom(child: ReadSet)
	}
	trait WriteSet {
		def wsCount: Int
		def wsRecWrite[T](handle: Handle[T], value: T)
		//def wsFind(handle: Handle[_]): Int
		//def wsGetSpecValue[T](i: Int): T
		def wsGet[T](handle: Handle[T]): Option[T]
		def wsGetSortedHandles: List[Handle[_]]
		def wsGetHandles: List[Handle[_]]
		def wsGetValueList: List[Tuple2[Handle[Any], Any]]
		def wsMergeFrom(child: WriteSet)
	}
	trait OpenCache {
		def ocCount: Int
		def ocRecOpen(obj: HObj)
		def ocGetCached[T <: HObj](id: String): Option[T]
		def ocMergeFrom(child: OpenCache)
	}
	trait DeferredRegister {
		def drRecRegister(obj: HObj)
		def drGetDeferred: Map[String,HObj]
	}
	trait OpenNestingRecord {
		def onrAddHandlers(onCommit: InTxn => Any, onAbort: InTxn => Any)
		def onrAddAbsLock(lock: String)
		def onrGetOnCommitHandlers: List[InTxn => Any]
		def onrGetOnAbortHandlers: List[InTxn => Any]
		def onrGetAbsLocks: List[String]
		def onrMergeFrom(child: OpenNestingRecord)
		def onrClear()
	}
}

object SimpleReadSetImpl {
	class Entry(val hash: Int, val handle: Handle[_])
}

trait SimpleReadSetImpl extends AccessHistory.ReadSet {
	private val _rs = mutable.ListBuffer[SimpleReadSetImpl.Entry]()

	def rsCount: Int = _rs.size
	def rsHandleSet: Set[Handle[_]] = _rs.toList.map(_.handle).toSet
	// TODO: Reuse objects! Maybe put back in array?
	def rsRecRead(handle: Handle[_]) { _rs += new SimpleReadSetImpl.Entry(MotSTM.hash(handle), handle) }
	def rsMergeFrom(child: AccessHistory.ReadSet) { _rs.appendAll(child.asInstanceOf[SimpleReadSetImpl]._rs) }
	def rsClear() { _rs.clear() }
}

object SimpleWriteSetImpl {
	class Entry(val hash: Int, val handle: Handle[_], val value: Any)
}

trait SimpleWriteSetImpl extends AccessHistory.WriteSet {
	private val _ws = mutable.Map[Int, SimpleWriteSetImpl.Entry]()

	def wsCount = _ws.size
	def wsRecWrite[T](handle: Handle[T], value: T) {
		// TODO: Reuse objects
		// TODO: question: Should we hash by identityHashCode of handle, 
		// handle.hashCode, or handle._hy_hash (i.e. hash by id) 
		val hash = handle._hy_hash
		_ws(hash) = new SimpleWriteSetImpl.Entry(hash, handle, value)
	}
	def wsGet[T](handle: Handle[T]): Option[T] = {
		val hash = handle._hy_hash
		val entry = _ws.get(hash)
		entry.map(_.value.asInstanceOf[T])
	}
	def wsGetSortedHandles(): List[Handle[_]] = _ws.keySet.toList.sorted.map(_ws(_).handle)
	// TODO: For wsGetHandles, why do we lookup every time?
	def wsGetHandles(): List[Handle[_]] = _ws.keySet.toList.map(_ws(_).handle) 
	//def wsGetHandles(): List[Handle[_]] = _ws.values.map(_.handle).toList
	def wsGetValueList(): List[Tuple2[Handle[Any], Any]] = {
		for ( (hash, entry) <- _ws.toList) 
			yield (entry.handle.asInstanceOf[Handle[Any]], entry.value)
	}
	def wsMergeFrom(child: AccessHistory.WriteSet) { _ws ++= child.asInstanceOf[SimpleWriteSetImpl]._ws }
	def wsClear() { _ws.clear() }
}

trait SimpleOpenCacheImpl extends AccessHistory.OpenCache {
	private val _oc = mutable.Map[String, HObj]()
	def ocCount = _oc.size
	def ocRecOpen(obj: HObj) { _oc(obj._id) = obj }
	def ocGetCached[T <: HObj](id: String): Option[T] = _oc.get(id).map(_.asInstanceOf[T])
	def ocGetHandles(): List[Handle[_]] = _oc.values.toList
	def ocMergeFrom(child: AccessHistory.OpenCache) { _oc ++= child.asInstanceOf[SimpleOpenCacheImpl]._oc }
	def ocClear() { _oc.clear() }
}

trait SimpleDeferredRegisterImpl extends AccessHistory.DeferredRegister {
	private val _dr = mutable.Map[String, HObj]()
	def drRecRegister(obj: HObj) { _dr(obj._id) = obj } 
	def drGetDeferred = _dr.toMap
	def drClear() { _dr.clear() } 
	def drMergeFrom(child: AccessHistory.DeferredRegister) { _dr ++= child.asInstanceOf[SimpleDeferredRegisterImpl]._dr }
}

trait SimpleOpenNestingRecordImpl extends AccessHistory.OpenNestingRecord {
		private val _ch = mutable.ListBuffer[InTxn => Any]()
		private val _ah = mutable.ListBuffer[InTxn => Any]()
		private val _al = mutable.ListBuffer[String]()
		def onrAddHandlers(onCommit: InTxn => Any, onAbort: InTxn => Any) {
			if (onCommit != null) _ch.append(onCommit)
			if (onAbort != null) _ch.append(onAbort)
		}
		def onrAddAbsLock(lock: String) {
			if (lock != null) _al.append(lock)
		}
		def onrGetOnCommitHandlers: List[InTxn => Any] = _ch.toList
		def onrGetOnAbortHandlers: List[InTxn => Any] = _ah.toList
		def onrGetAbsLocks: List[String] = _al.toList
		def onrMergeFrom(child: AccessHistory.OpenNestingRecord) {
			val c = child.asInstanceOf[SimpleOpenNestingRecordImpl]
			_ch.appendAll(c._ch)
			_ah.appendAll(c._ah)
			_al.appendAll(c._al)
		}
		def onrClear() {
			_ch.clear()
			_ah.clear()
			_al.clear()
		}
	}

trait AccessHistory extends SimpleReadSetImpl 
	with SimpleWriteSetImpl 
	with SimpleOpenCacheImpl 
	with SimpleDeferredRegisterImpl 
	with SimpleOpenNestingRecordImpl {
	
	def parLevel: AccessHistory
	//var consumedRetryDelta = 0L
	
	def mergeFrom(child: AccessHistory) {
		rsMergeFrom(child)
		wsMergeFrom(child)
		ocMergeFrom(child)
		drMergeFrom(child)
		onrMergeFrom(child)
		//consumedRetryDelta += child.consumedRetryDelta
	}
	
	def clear() {
		rsClear()
		wsClear()
		ocClear()
		drClear()
		onrClear()
	}
	
	/*@tailrec final def consumedRetryTotal(accum: Long = 0L): Long = {
      val z = accum + consumedRetryDelta
      if (parLevel == null) z else parLevel.consumedRetryTotal(z)
    }*/
}
