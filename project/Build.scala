import sbt._
import Keys._
import com.typesafe.sbtmultijvm.MultiJvmPlugin
import com.typesafe.sbtmultijvm.MultiJvmPlugin.{ MultiJvm, extraOptions }
import com.github.retronym.SbtOneJar

import sbtassembly.Plugin._
import AssemblyKeys._




// Define resolvers and dependencies
object Resolvers {
  val typesafeRepo = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases"
  val scalatoolsRepo = "Scala-tools.org Repository" at "http://scala-tools.org/repo-releases/"
  val mavenRepo = "Maven Repo" at "http://repo2.maven.org/maven2/"
  
  val all = Seq(typesafeRepo, scalatoolsRepo, mavenRepo)
}


object Dependencies {
  val akka = 		"com.typesafe.akka" % 	"akka-actor" 	% "2.0" 
  val akkaRemote = 	"com.typesafe.akka" % 	"akka-remote"	% "2.0"
  val akkaSlf4j = 	"com.typesafe.akka" % 	"akka-slf4j"	% "2.0" 
  val akkaTestkit = "com.typesafe.akka" % 	"akka-testkit" 	% "2.0" 	% "test"

  val scalaStm = 	"org.scala-tools" 	%% 	"scala-stm" 	% "0.5"
  val loglady = 	"org.eintr.loglady" %% 	"loglady" 		% "1.0.0"
  val scalatest = 	"org.scalatest" 	%% "scalatest" 		% "1.7.1" 	% "test"

  val kryo = 		"com.googlecode" 	% "kryo"		% "1.04"
  val kryoSer = 	"de.javakaffee"		% "kryo-serializers"	% "0.9"
  val configrity = 	"org.streum" 		%%	"configrity-core" % "0.10.0"  
 
  val all = Seq(akka, akkaRemote, akkaSlf4j, akkaTestkit, scalaStm, loglady, scalatest, configrity, kryo, kryoSer)
}    

// Build settings
object BuildSettings {
  val buildOrganization = "vt.edu"
  val buildVersion      = "0.0.1"
  val buildScalaVersion = "2.9.1"

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion,
    exportJars := true
  )
  
  val extraSettings = Seq(
    resolvers ++= Resolvers.all,
    libraryDependencies ++= Dependencies.all
  ) ++ SbtOneJar.oneJarSettings
}

// Define the project structure
object HyflowBuild extends Build {
  import BuildSettings._

  lazy val hyflow = Project (
    id = "hyflow",
    base = file ("."),
    settings = buildSettings
  ) aggregate (core, benchmarks)

  lazy val core = Project (
    id = "hyflow-core",
    base = file ("hyflow-core"),
    settings = buildSettings ++ extraSettings ++ MultiJvmPlugin.settings ++ Seq(
      extraOptions in MultiJvm <<= (sourceDirectory in MultiJvm) { src =>
         (name: String) => (src ** (name + ".conf")).get.headOption.map("-Dconfig.file=" + _.absolutePath).toSeq
      },
      test in Test <<= (test in Test) dependsOn (test in MultiJvm)
    )
  ) configs (MultiJvm)
			     
  lazy val benchmarks = Project (
    id = "hyflow-benchmarks",
    base = file ("hyflow-benchmarks"),
    settings = buildSettings ++ extraSettings
  ) dependsOn (core)
  
}

