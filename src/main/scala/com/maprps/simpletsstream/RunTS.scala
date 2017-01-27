package com.maprps.simpletsstream

import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegressionModel


object RunTS extends Serializable {

    case class runParams (modelLoc: String = null)
    val assembler = new VectorAssembler()
        .setInputCols(Array("r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9",
            "r10", "r11", "r12", "r13", "r14", "r15", "r16", "ethylene",
            "vr1", "vr2", "vr3", "vr4", "vr5", "vr6", "vr7", "vr8", "vr9", "vr10",
            "vr11", "vr12", "vr13", "vr14", "vr15", "vr16", "vethylene",
            "ar1", "ar2", "ar3", "ar4", "ar5", "ar6", "ar7", "ar8", "ar9", "ar10",
            "ar11", "ar12", "ar13", "ar14", "ar15", "ar16", "aethylene"
        ))
        .setOutputCol("features")

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
            opt[String]("modelLoc")
              .text("input the model location")
              .action((x, c) => c.copy(modelLoc = x))
              .required()
        }
        parser.parse(args, defaultParam).map { params =>
            run(sc, params.modelLoc)
        } getOrElse {
            sys.exit(1)
        }
        // sc.stop()
    }

    def run(sc: SparkContext, modelLoc: String): Unit = {
        val tsSchema = List( "ethylene", "r1", "r2", "r3", "r4",
            "r5", "r6", "r7", "r8", "r9", "r10", "r11", "r12",
            "r13", "r14", "r15", "r16", "prediction")
        val ssc = new StreamingContext(sc, Seconds(5))
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
        val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka
        val groupId = "testgroup"
        val offsetReset = "earliest"
        val pollTimeout = "1000"
        val topics = "/sample-stream:sensor1-region1"
        val topicSet = topics.split(",").toSet

        val kafkaParams = Map[String, String](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
            ConsumerConfig.GROUP_ID_CONFIG -> groupId,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
                "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
                "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
            "spark.kafka.poll.time" -> pollTimeout
        )
        val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
        val timeStamp = System.currentTimeMillis
        val lines = KafkaUtils.createDirectStream[String, String](ssc,
            LocationStrategies.PreferConsistent, consumerStrategy).map(_.value())
        // val trainingDataToTSDB = ssc.textFileStream("maprfs:///user/mapr/train")
        lines.foreachRDD( rdd => {
            val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
            import spark.implicits._
            val data = rdd.map(r => r.split(',')).toDF("ts",
                "ethylene", "r1", "r2", "r3", "r4",
                "r5", "r6", "r7", "r8", "r9", "r10", "r11", "r12",
                "r13", "r14", "r15", "r16")
            data.createOrReplaceTempView("dev")
            val dataDf = spark.sql(
                """select t1.ts, t1.r1, t1.r2, t1.r3, t1.r4,
                  |t1.r5, t1.r6, t1.r7, t1.r8, t1.r9, t1.r10, t1.r11, t1.r12,
                  			|t1.r13, t1.r14, t1.r15, t1.r16, t1.ethylene,
                  			|t3.vr1, t3.vr2, t3.vr3, t3.vr4, t3.vr5, t3.vr6, t3.vr7,
                  |t3.vr8, t3.vr9, t3.vr10, t3.vr11, t3.vr12,
                  	  |t3.vr13, t3.vr14, t3.vr15, t3.vr16, t3.vethylene,
                  |t3.ar1, t3.ar2, t3.ar3, t3.ar4, t3.ar5, t3.ar6, t3.ar7,
                  |t3.ar8, t3.ar9, t3.ar10, t3.ar11, t3.ar12,
                  	  |t3.ar13, t3.ar14, t3.ar15, t3.ar16, t3.aethylene
                  |from (select *, 1 as index from dev) t1
                  |full outer join (
                  |		select variance(r1) as vr1, avg(r1) as ar1,
                  |     variance(r2) as vr2, avg(r2) as ar2,
                  |     variance(r3) as vr3, avg(r3) as ar3,
                  |     variance(r4) as vr4, avg(r4) as ar4,
                  |     variance(r5) as vr5, avg(r5) as ar5,
                  |     variance(r6) as vr6, avg(r6) as ar6,
                  |     variance(r7) as vr7, avg(r7) as ar7,
                  |     variance(r8) as vr8, avg(r8) as ar8,
                  |     variance(r9) as vr9, avg(r9) as ar9,
                  |     variance(r10) as vr10, avg(r10) as ar10,
                  	  |     variance(r11) as vr11, avg(r11) as ar11,
                  	  |     variance(r12) as vr12, avg(r12) as ar12,
                  	  |     variance(r13) as vr13, avg(r13) as ar13,
                  	  |     variance(r14) as vr14, avg(r14) as ar14,
                  	  |     variance(r15) as vr15, avg(r15) as ar15,
                  	  |     variance(r16) as vr16, avg(r16) as ar16,
                  |     variance(ethylene) as vethylene, avg(ethylene) as aethylene,
                  |     1 as index
                  |  	from dev
                  |) t3 on (t1.index = t3.index)
                """.stripMargin
            )

            val output = assembler.transform(dataDf).cache()
            val lrModel = LinearRegressionModel.load(modelLoc)
            val score = lrModel.transform(output.select("features", "ts",
                "ethylene", "r1", "r2", "r3", "r4",
                "r5", "r6", "r7", "r8", "r9", "r10", "r11", "r12",
                "r13", "r14", "r15", "r16" ))

            score.select("ts",
                "ethylene", "r1", "r2", "r3", "r4",
                "r5", "r6", "r7", "r8", "r9", "r10", "r11", "r12",
                "r13", "r14", "r15", "r16", "prediction" ).flatMap( s => {
                val parts: Seq[String] = s.toSeq.map(_.toString)
                val l = parts.length
                var tsdbMetrics = new ListBuffer[String]()
                for ( i <- 1 to l-2) {
                    tsdbMetrics += tsSchema(i-1) +" " +(parts(0).toDouble * 1000 + timeStamp )
                        .toLong.toString +" " + parts(i) +" SENSOR=sensor1 REGION=region1"
                }
                tsdbMetrics += tsSchema(l-2) + " " + ((parts(0).toDouble + 5) * 1000 + timeStamp )
                    .toLong.toString +" " + parts(l-1) + " SENSOR=sensor1 REGION=region1"
                tsdbMetrics.toList
            } ).mapPartitions(OpenTSDB.toTSDB).collect
            printf("------------------------")
            printf(rdd.take(1).mkString(",") + "/n")
            printf("------------------------")
        } )
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
   }
}

        
