// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

spName := "com.maprps/simple-ts-stream"

organization := "com.maprps"

version := "0.1.0"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.6.1"

sparkComponents ++= Seq("mllib", "sql", "core", "graphx", "streaming")

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

test in assembly := {}

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

// Can't parallelly execute in test
parallelExecution in Test := false

fork in Test := true

javaOptions ++= Seq("-Xmx2G", "-XX:MaxPermSize=256m")

libraryDependencies ++= Seq(
		"junit" % "junit" % "4.12",
		"org.scalatest" %% "scalatest" % "2.2.6" % "test",
		"com.databricks" % "spark-csv_2.11" % "1.4.0"
)

resolvers ++= Seq(
		"mapr-repo" at "http://repository.mapr.com/maven"
)
