package org.hyflow

import org.hyflow.api._
import org.hyflow.core.util._
import org.hyflow.core._
import org.hyflow.core.store._
import akka.actor._
import ch.qos.logback.classic.util
import scala.collection.mutable
import java.io.File
import akka.routing._
import akka.dispatch.{ Dispatchers, Future, Await }
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import com.typesafe.config.{ Config, ConfigFactory }
import org.eintr.loglady.Logging

// Hyflow main actor (TODO: do we even need this class?)
private class Hyflow extends Actor with Logging {
	def receive() = {
		// This should never get called. Hyflow actor routes instead of receiving messages.
		case msg @ _ =>
			
			log.warn("Unexpected message received by mainActor. | msg = %s", msg)
	}
}

case class HyflowRouter() extends RouterConfig with Logging {
	def routerDispatcher: String = Dispatchers.DefaultDispatcherId
	override val supervisorStrategy = OneForOneStrategy() {
		case e: Throwable =>
			log.error(e, "Exception thrown by Hyflow service.")
			SupervisorStrategy.Escalate
	}

	def createRoute(routeeProps: Props, routeeProvider: RouteeProvider): Route = {
		// Go through all Hyflow Services and initialize actors for them
		val routees = Hyflow.services.map(svc => {
			svc.ref = routeeProvider.context.actorOf(svc.actorProps, svc.name)
			svc.ref
		})

		// Register all newly initialized actors with the routee provider
		routeeProvider.registerRoutees(routees.toArray)

		// TODO: cache the services list into a constant time lookup hashtable or trie
		// ATTENTION: this must be reentrant
		val res = new Route {
			def isDefinedAt(x: (ActorRef, Any)) = true
			def apply(x: (ActorRef, Any)): Iterable[Destination] = {
				val (sender, message) = x

				// execute message payload callback (must be reentrant)
				if (message.isInstanceOf[Message]) {
					val hymsg = message.asInstanceOf[Message]
					Hyflow.processMessagePayloads(hymsg)
				}

				// redirect message
				Hyflow.services.find(_.accepts(message)) match {
					case Some(svc) =>
						List(Destination(sender, svc.ref))
					case None =>
						// Discard message
						log.warn("Unexpected message received by mainActor. Discarded. | msg = %s", 
								message)
						List()
				}
			}
		}
		res
	}
}

/**
 * Main Hyflow object, app entry point
 */
object Hyflow extends Logging {
	
	implicit val timeout = Timeout(5 seconds)

	val akkaConfigFile = new java.io.File("etc/application.conf")

	// Initialize the actor system
	var system: ActorSystem = null

	// Hyflow main actor
	var mainActor: ActorRef = null
  var groupLeader: ActorRef = null
	// Effectively unique int identifying this node
	val _hy_nodeid = scala.util.Random.nextInt

	// Hyflow service list
	val services = mutable.ListBuffer[Service]()

	// Hyflow message payloads
	var payloads = List[PayloadHandler]()

	// Hyflow router, selects first service that can handle the message
	// TODO: this throws exception if it isn't lazy. TODO: check, when are we actually using this?
	var routerActor: ActorRef = null

	// Known peers
	val peers = mutable.ListBuffer[ActorRef]()

	// Lock & Store & Dir providers
	var locks: LockProvider = null
	var store: StoreProvider = null
	var dir: Directory = null

	// Callbacks
	val onInit = mutable.ListBuffer[() => Unit]()
	val onReady = mutable.ListBuffer[() => Unit]()
	val onShutdown = mutable.ListBuffer[() => Unit]()
	
	// Stats (approximate, aren't synchronized for correctness)
	@volatile var _topAborts: Int = 0
	@volatile var _topCommits: Int = 0
	@volatile var _openNestedAborts: Int = 0
	@volatile var _openNestedCommits: Int = 0
	@volatile var _closedNestedAborts: Int = 0
	@volatile var _closedNestedCommits: Int = 0
	

	/**
	 * Initialize Hyflow here.
	 */
	def init(args: Array[String], userServices: List[Service] = List()) {
		log.info("Initializing Hyflow.")

		// Configure port
		val akkaConf = ConfigFactory.parseFile(akkaConfigFile)
		val newPort = HyflowConfig.cfg[Int]("hyflow.basePort") + HyflowConfig.cfg[Int]("id")
		val hostname = HyflowConfig.cfg[String]("hyflow.hostname")
		val confOverrides = ConfigFactory.parseString(
				"akka.remote.netty.port = %d\n".format(newPort) +
				"akka.remote.netty.hostname = \"%s\"\n".format(hostname)
		)

		// Initialize akka actor system
		val classLoader = getClass.getClassLoader
		system = ActorSystem("hyflow", confOverrides.withFallback(akkaConf), classLoader)

		// Hard-wire MotSTM DTM implementation
		//scala.concurrent.stm.impl.STMImpl.select(scala.concurrent.stm.motstm.MotSTM)
		//HyflowBackendAccess.configure(scala.concurrent.stm.motstm.HyMot)
		//HRef.factory = scala.concurrent.stm.motstm.MotSTM
		
		// Hard-wire HaiSTM DTM implementation
		//scala.concurrent.stm.impl.STMImpl.select(scala.concurrent.stm.haistm.HaiSTM)
		//HyflowBackendAccess.configure(scala.concurrent.stm.haistm.HyHai)
		//HRef.factory = scala.concurrent.stm.haistm.HaiSTM
		
		registerPayloadHandler(scala.concurrent.stm.motstm.TFAClock)
		
		

		// Configure locks provider
		val locksProvider = HyflowConfig.cfg[String]("hyflow.modules.locks") match {
			case "Combined_LockStore" => org.hyflow.core.store.Combined_LockStore
			//case "Separated_Lock" => org.hyflow.core.store.Separated_Lock
      case "CRF_LockStore" => org.hyflow.core.store.CRF_LockStore
      case "Replicated_LockStore" => org.hyflow.core.store.Replicated_LockStore
			case s => throw new Exception("Unknown lock provider: " + s)
		}
		locks = locksProvider
		registerService(locksProvider)

		// Configure store provider
		val storeProvider = HyflowConfig.cfg[String]("hyflow.modules.store") match {
			case "Combined_LockStore" => org.hyflow.core.store.Combined_LockStore
			//case "Separated_Store" => org.hyflow.core.store.Separated_Store
      case "CRF_LockStore" => org.hyflow.core.store.CRF_LockStore
      case "Replicated_LockStore" => org.hyflow.core.store.Replicated_LockStore
			case s => throw new Exception("Unkown store provider: " + s)
		}
		store = storeProvider
		registerService(storeProvider)

		/*val dirProvider = HyflowConfig.cfg[String]("hyflow.modules.directory") match {
			case "Tracker" => org.hyflow.core.directory.Tracker
			case s => throw new Exception("Unknown directory: " + s)
		}
		dir = dirProvider
		registerService(dirProvider)*/

		// Hard-wire other hyflow services
		registerService(org.hyflow.core.LifecycleManager)
		registerService(org.hyflow.core.BarrierService)
		log.debug("User services. | userServices = %s", userServices)
		for (usrv <- userServices)
			registerService(usrv)

		// Create main actor with router
		mainActor = system.actorOf(Props[Hyflow].withRouter(HyflowRouter()), name = "hyflow")
		log.debug("Main actor created. | mainActor = %s", mainActor)
		//routerActor = system.actorOf(Props().withRouter(HyflowRouter()))

		if (HyflowConfig.cfg.get[Int]("id") == Some(0)) {
			log.debug("We are the coordinator node.")
		}

		// greet the coordinator (even if it's just this node's mainActor)
		val coord = system.actorFor("akka://hyflow@%s:%d/user/hyflow".format(
			HyflowConfig.cfg[String]("hyflow.coord"),
			HyflowConfig.cfg[Int]("hyflow.basePort")))

		coord ! new LifecycleManager.HelloMsg(Hyflow.mainActor)

		for (initCallback <- onInit)
			initCallback()

		log.info("Initialization complete.")
	}
  def assignLeader(){
    groupLeader =  Hyflow.peers(0)
  }
	/**
	 * Register a HyflowService
	 */
	def registerService(s: Service) {
		if (services.exists(_.name == s.name)) {
			log.debug("Service already registered. | name: %s | class: %s", s.name, s.getClass())
		} else {
			log.info("Registering Hyflow service. | name: %s | class: %s", s.name, s.getClass())
			services.append(s)
		}
	}

	/**
	 * Register a payload handler
	 * TODO: extract this into a trait
	 */
	def registerPayloadHandler(h: PayloadHandler) {
		log.info("Registering payload handler. | name: %s | class: %s", h.name, h.getClass())
		payloads = payloads :+ h
	}

	/**
	 * Stop running services.
	 */
	def shutdown() {
		log.info("Shutting down.")
		system.shutdown()
		for (shutdownCallback <- onShutdown)
			shutdownCallback()
	}

	private[hyflow] def processMessagePayloads(hymsg: Message) {
		//log.trace("Processing payloads. | message: %s | class: %s", hymsg, hymsg.getClass())
		for (handler <- Hyflow.payloads)
			//if (hymsg.payloads.contains(handler.name))
			handler.incoming(hymsg)
	}

	private[hyflow] def callOnReady() {
		for (readyCallback <- onReady) {
			readyCallback()
		}
	}

	def done() {
		peers(0) ! new LifecycleManager.DoneMsg()
	}

	def barrier(id: String) {
		log.info("Awaiting on barrier. | id = %s", id)
		val f = ask(Hyflow.peers(0), BarrierService.BarrierMsg(id, HyflowConfig.cfg[Int]("hyflow.nodes")))(120 seconds)
		Await.ready(f, 120 seconds)
		log.info("Barrier done. | id = %s", id)
	}
}

object HyflowMain {

	/**
	 * Test routine, not used in production as Hyflow is a library
	 * TODO: move this to the benchmarks package
	 */
	def main(args: Array[String]) {
		/*try {
			// Hard-wired configuration object 
			//(do we need to reference it to force the load?")
			HyflowConfig.init(args)
			
			// Inform libraries about the location of the configuration files
			// TODO: Maybe make this configurable?

			println("Entering Hyflow test routine.")
			println("main() | cwd: %s | args: %s", new File("") getAbsolutePath, args.mkString("[", ", ", "]"))

			// Register callbacks
			Hyflow.onInit += Example.init _
			Hyflow.onReady += Example.trigger _
			Hyflow.onShutdown += Example.onEnd _

			Hyflow.init(args)

			Hyflow.system.awaitTermination()
		} catch {
			case e =>
				e.printStackTrace()
				if (Hyflow.system != null)
					Hyflow.shutdown()
		}

		println("Hyflow test routine ended.")*/
		exit(0)
	}
}
