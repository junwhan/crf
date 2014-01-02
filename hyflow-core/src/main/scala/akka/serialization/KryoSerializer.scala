package akka.serialization

import com.typesafe.config.ConfigFactory
import com.esotericsoftware.kryo._
import de.javakaffee.kryoserializers._
import com.esotericsoftware.kryo.serialize.SimpleSerializer
import java.nio.ByteBuffer
import akka.actor._
import akka.routing._
import akka.remote._
import org.hyflow.api._
import scala.collection.mutable

private class ActorRefSerializer(val kryo: Kryo, val system: ExtendedActorSystem) extends SimpleSerializer[ActorRef] {
	override def read(buffer: ByteBuffer): ActorRef = {
		val path = kryo.readObject(buffer, classOf[String])
		system.actorFor(path)
	}

	override def write(buffer: ByteBuffer, ref: ActorRef) {
		Serialization.currentTransportAddress.value match {
			case null => kryo.writeObject(buffer, ref.path.toString)
			case addr => kryo.writeObject(buffer, ref.path.toStringWithAddress(addr))
		}
	}
}

class KryoSerializer(val system: ExtendedActorSystem) extends Serializer {
	val buffer = new ThreadLocal[ObjectBuffer] {
		override def initialValue() = {
			val kryo = new KryoReflectionFactorySupport
			kryo.setRegistrationOptional(true)
			kryo.register(classOf[RoutedActorRef], new ActorRefSerializer(kryo, system))
			kryo.register(classOf[RemoteActorRef], new ActorRefSerializer(kryo, system))
			new ObjectBuffer(kryo)
		}
	}

	// This is whether "fromBinary" requires a "clazz" or not
	def includeManifest: Boolean = false

	def identifier = 4747835

	def toBinary(obj: AnyRef): Array[Byte] = {
		buffer.get.writeClassAndObject(obj)
	}

	def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
		buffer.get.readClassAndObject(bytes)
	}
}
