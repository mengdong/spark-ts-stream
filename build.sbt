name := "Sample TS Application" 

organization := "MapR Technologies" 

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  // testing
  "org.scalatest"   %% "scalatest"    % "2.2.4"   % "test,it",
  "org.scalacheck"  %% "scalacheck"   % "1.12.2"      % "test,it",
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // spark core
  "org.apache.spark" % "spark-core_2.10" % "1.5.2",
  "org.apache.spark" % "spark-graphx_2.10" % "1.5.2",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.2",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.2",
  // spark packages
  "com.databricks" % "spark-csv_2.10" % "1.3.0"
)

//mainClass in (Compile, packageBin) := Some("com.mapr.tsapp.examples")

scalacOptions ++= List("-feature","-deprecation", "-unchecked", "-Xlint")

resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/" // allows us to include spark packages

resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"
// Compiler settings. Use scalac -X for other options and their description.
// See Here for more info http://www.scala-lang.org/files/archive/nightly/docs/manual/html/scalac.html
scalacOptions ++= List("-feature","-deprecation", "-unchecked", "-Xlint")

