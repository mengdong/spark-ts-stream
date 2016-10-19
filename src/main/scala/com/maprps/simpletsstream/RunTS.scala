package com.maprps.simpletsstream

import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
// import org.apache.spark.streaming.kafka._
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors

object RunTS extends Serializable {

    case class runParams (param1: String = null)

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // conf.registerKryoClasses(Array(classOf[class1], classOf[class2]))
        val sc = new SparkContext(conf)

        val defaultParam = new runParams()
        val parser = new OptionParser[runParams](this.getClass.getSimpleName) {
            head(s"${this.getClass.getSimpleName}: Run simple app")
            opt[String]("param1")
              .text("sample_param1")
              .action((x, c) => c.copy(param1 = x))
              .required()
        }
        parser.parse(args, defaultParam).map { params =>
            run(sc, params.param1)
        } getOrElse {
            sys.exit(1)
        }
        // sc.stop()
    }

    def run(sc: SparkContext, param1: String): Unit = {
        // val param1 = "sample content of param1"
        // got SparkContext, just do something
        val tsSchema = List( "ethylene", "r1", "r2", "r3", "r4",
            "r5", "r6", "r7", "r8", "r9", "r10", "r11", "r12",
            "r13", "r14", "r15", "r16")
        val ssc = new StreamingContext(sc, Seconds(60))
        ssc.checkpoint("maprfs:///checkpoint/.")
/*
        val trainingData = ssc.textFileStream("maprfs:///user/mapr/train").map( s => {
        // val trainingData = sc.textFile("maprfs:///data").map( s => {
            val parts = s.split(',')
            val l = parts.length
            val label = java.lang.Double.parseDouble(parts(1))
            val features = Vectors.dense(parts.slice(2, l).map(java.lang.Double.parseDouble))
            LabeledPoint(label, features)
        }).cache()
        val testData = ssc.textFileStream("maprfs:///user/mapr/test").map( s => {
            val parts = s.split(',')
            val l = parts.length
            val label = java.lang.Double.parseDouble(parts(1))
            val features = Vectors.dense(parts.slice(2, l).map(java.lang.Double.parseDouble))
            LabeledPoint(label, features)
        })
*/
        val trainingDataToTSDB = ssc.textFileStream("maprfs:///user/mapr/train")
            .flatMap( s => {
                val parts = s.split(',')
                val l = parts.length
                var tsdbMetrics = new ListBuffer[String]()
                for ( i <- 1 to l) {
                    tsdbMetrics += tsSchema(i-1) +" " +(parts(0).
                        toDouble * 100 + 1476868264).toInt.toString +" " + parts(i) +" sensor1"
                }
                tsdbMetrics.toList
            })
            .mapPartitions(OpenTSDB.toTSDB)
/*
        val numFeatures = 16
        val model = new StreamingLinearRegressionWithSGD()
            .setInitialWeights(Vectors.zeros(numFeatures))

        model.trainOn(trainingData)
        model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
*/
        ssc.start()
        ssc.awaitTermination()

        ssc.stop()
        /*
        val topicSet = Set("test")
        val brokers = "localhost:9092"
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
        val messages = KafkaUtils.createDirectStream(ssc, kafkaParams, topicSet)
        */
    }
}

        
