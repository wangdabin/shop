name := "sbtSpark"

version := "1.0"

scalaVersion := "2.10.4"

organization := "com.lenovo.btit.dtcs.cida.dmd"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.5.1",
  "org.apache.spark" %% "spark-hive" % "1.5.1",
  "org.apache.spark" %% "spark-mllib" % "1.5.1"
)
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.36"
libraryDependencies += "com.googlecode.combinatoricslib" % "combinatoricslib" % "2.1"
//libraryDependencies += "org.apache.spark" % "spark-hive-thriftserver_2.10" % "1.5.1"



//libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.1"
//libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.1"