import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._

assemblySettings
name := "ddi"
organization := "com.yarenty"
version := "1.0"

// scalaVersion := "2.11.8"
scalaVersion := "2.10.6"     // h2o is not ported yet to scala 2.11

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

jarName in assembly := "ddi.jar"
mainClass in assembly := Some("com.yarenty.ddi.MLProcessor")
// no scala classes in assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"  % "provided"

libraryDependencies += "ai.h2o" % "sparkling-water-core_2.10" % "1.6.5"

libraryDependencies += "ai.h2o" % "sparkling-water-ml_2.10" % "1.6.5"

//assembly issues resolving
libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.6.1" % "provided" intransitive()

//RESOLVERS
//resolvers += "Local Maven Repository" at "" + Path.userHome.asFile.toURI.toURL + ".m2/repository"


//NO TEST when assembly
test in assembly := {}

//-------------------
//all for test
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "junit" % "junit" % "4.12" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".RSA" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith "mailcap" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".thrift" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".jnilib" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".dll" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".so" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "log4j.properties" => MergeStrategy.discard
  case x => old(x)
}
}