package org.hyflow

import org.scalatest._
import akka.actor._
import akka.serialization._

import org.hyflow.api._
import org.hyflow.core._
import org.hyflow.core.util._

case class Test1(val id: String) {
	
	val payloads: Map[String, Any] = Map()
}

class KryoTest extends FunSuite {
	test("Kryo serialization") {
		HyflowConfig.init(Array())
		
		val system = ActorSystem("test")
		Hyflow.registerService(org.hyflow.core.LifecycleManager)
		val actor = system.actorOf(Props[Hyflow].withRouter(HyflowRouter()), name = "hyflow")
		
		val orig = new LifecycleManager.HelloMsg(actor)
		
		val serializer = new KryoSerializer(system.asInstanceOf[ExtendedActorSystem])
		
		val bytes = serializer.toBinary(orig)
		val deser = serializer.fromBinary(bytes, None)//.asInstanceOf[Test1]
		
		println(orig, deser)
		//println(orig.payloads, deser.payloads)
		
		assert(orig == deser)
		//assert(orig.payloads == deser.payloads)
	}
}