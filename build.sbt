import _root_.sbtassembly.Plugin.AssemblyKeys
import _root_.sbtassembly.Plugin._
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

assemblySettings

name := "sbtSpark"

version := "1.0"

scalaVersion := "2.10.4"

organization := "com.lenovo.btit.dtcs.cida.dmd"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided",
//  "org.elasticsearch" % "elasticsearch" % "1.7.0",
  "com.googlecode.combinatoricslib" % "combinatoricslib" % "2.1",
  "mysql" % "mysql-connector-java" % "5.1.36"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
  case PathList("com", "twitter", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", "minlog", xs @ _*) => MergeStrategy.last
  case "META-INF/maven/org.apache.avro/avro-ipc/pom.properties" => MergeStrategy.last
  case "META-INF/maven/org.slf4j/slf4j-api/pom.xml" => MergeStrategy.last
  case "META-INF/maven/org.slf4j/slf4j-api/pom.properties" => MergeStrategy.last
  case "com/esotericsoftware/minlog/Log$Logger.class" => MergeStrategy.last
  case x => old(x)
}
}
