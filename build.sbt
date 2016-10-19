// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

spName := "com.maprps/simpletsstream"

organization := "com.maprps"

version := "0.1.0"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.6.1"

sparkComponents ++= Seq("mllib", "sql", "core", "graphx", "streaming", "streaming-kafka")

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

test in assembly := {}

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

val hbaseVersion = "1.1.1-mapr-1602"
val hadoopVersion = "2.7.0-mapr-1602"

// Can't parallelly execute in test
parallelExecution in Test := false

fork in Test := true

javaOptions ++= Seq("-Xmx2G", "-XX:MaxPermSize=512m")

libraryDependencies ++= Seq(
		"com.databricks" % "spark-csv_2.10" % "1.5.0",
        "com.github.scopt" %% "scopt" % "3.5.0",
        "org.apache.kafka" % "kafka_2.10" % "0.9.0.0",
        "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
        "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided",
        "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
        "com.sun.jersey" % "jersey-client" % "1.18.3"
)

resolvers ++= Seq(
		"mapr-repo" at "http://repository.mapr.com/maven"
)
