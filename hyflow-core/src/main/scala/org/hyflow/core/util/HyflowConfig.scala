package org.hyflow.core.util

import scala.io.Source
import org.streum.configrity._
import java.net.URL
import java.io.IOException
import scala.collection.mutable

object HyflowConfig {
	// Set properties here in the lack of a better alternative
	System.setProperty("logback.configurationFile", new java.io.File("etc/logback.xml").getAbsolutePath)

	// Some constants
	var CONFIG = System.getProperty("hyflow.config", "etc/hyflow.conf")
	var IDENTITY = System.getProperty("hyflow.id", "0")

	def init(args: Array[String]): mutable.Map[String, String] = {
		val extra = mutable.Map[String, String]()
		for (arg <- args) {
			val res = arg.split("=", 2)
			if (res.length == 2) {
				res(0) match {
					case "config" => CONFIG = res(1)
					case "id" => IDENTITY = res(1)
					case _ => extra(res(0)) = res(1)
				}
			}
		}
		cfg = Configuration(extra.toMap) include loadCfg(CONFIG) 
		System.setProperty("hyflow_nodeid", HyflowConfig.cfg[String]("id"))
		System.setProperty("hyflow_hostname", HyflowConfig.cfg[String]("hyflow.logging.hostname"))
		extra
	}

	// Load function
	def loadCfg(cfgFile: String) = {
		//log.debug("Loading configuration. | file: %s", cfgFile)
		// Start from the node's identity
		var res = Configuration("id" -> IDENTITY)
		// Add in the local configuration file
		try {
			val localConf = Configuration.load(cfgFile)
			res = res include localConf
			// Load defaults from URL
			if (localConf("config.loadFromServer", false)) {
				val cfgurl = localConf[String]("config.loadFromServer_URL")
				//log.debug("Requested loading of configuration from server. | url: %s", cfgurl)
				// Config root
				val url = Source fromURL (new URL(cfgurl))
				val remoteConf = Configuration.load(url)
				res = res include remoteConf
				// Current node
				val nodePath = "nodes." + IDENTITY
				val nodeConf = remoteConf.detach(nodePath)
				res = res include nodeConf
			}
		} catch {
			case e: IOException => println("Could not load configuration. | err: " + e.getMessage())
		}

		// Return configuration
		//log.trace("Using the following configuration:\n%s", res.format())
		res
	}

	// Config store
	var cfg = Configuration()

	// Shortcuts and defaults
	lazy val basePort = cfg("hyflow.comm.basePort", 42600)
}
