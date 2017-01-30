package com.maprps.simpletsstream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import scopt.OptionParser

object Regression{

    case class runParams (train: String = null, resultPath: String = null)

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

        val spark = SparkSession.builder()
			.appName("TrainPMML")
			.master("local")
		    .getOrCreate()
        val defaultParam = new runParams()
        val parser = new OptionParser[runParams](this.getClass.getSimpleName) {
            head(s"${this.getClass.getSimpleName}: Run simple app")
            opt[String]("train")
                .text("location of the training data")
                .action((x, c) => c.copy(train = x))
                .required()
			opt[String]("resultPath")
			    .text("location of the model")
			    .action((x, c) => c.copy(resultPath = x))
			    .required()
        }
        parser.parse(args, defaultParam).map { params =>
            run(spark, params.train, params.resultPath)
        } getOrElse {
            sys.exit(1)
        }
    }

    def run(spark: SparkSession, train: String, resultPath: String): Unit = {
		val data = spark.read.option("header", "true").option("inferSchema", "true")
			.format("com.databricks.spark.csv").load(train)
		data.createOrReplaceTempView("train")
		val df = spark.sql(
			"""select log(t2.ethylene + 0.05) as label, t1.ts, t1.r1, t1.r2, t1.r3, t1.r4,
            |t1.r5, t1.r6, t1.r7, t1.r8, t1.r9, t1.r10, t1.r11, t1.r12,
			|t1.r13, t1.r14, t1.r15, t1.r16, t1.ethylene,
			|t3.vr1, t3.vr2, t3.vr3, t3.vr4, t3.vr5, t3.vr6, t3.vr7,
            |t3.vr8, t3.vr9, t3.vr10, t3.vr11, t3.vr12,
	  |t3.vr13, t3.vr14, t3.vr15, t3.vr16, t3.vethylene,
      |t3.ar1, t3.ar2, t3.ar3, t3.ar4, t3.ar5, t3.ar6, t3.ar7,
      |t3.ar8, t3.ar9, t3.ar10, t3.ar11, t3.ar12,
	  |t3.ar13, t3.ar14, t3.ar15, t3.ar16, t3.aethylene
      |from train t1 inner join (
      |		select ts+60 as ts_future, ethylene
      |  	from train
      |) t2 on (t1.ts = t2.ts_future)
      |inner join (
      |		select floor(ts/20) as interval, variance(r1) as vr1, avg(r1) as ar1,
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
      |     variance(ethylene) as vethylene, avg(ethylene) as aethylene
      |  	from train
      |   	group by floor(ts/20)
      |) t3 on (floor(t1.ts/20) = t3.interval)
    """.stripMargin
		)

		val lr = new LinearRegression()
			.setMaxIter(10)
			.setRegParam(0.3)
			.setElasticNetParam(0.8)
		val output = assembler.transform(df).select("features", "label").cache()
		val lrModel = lr.fit(output)
		lrModel.save(resultPath)
		printf(s"saving result to $resultPath")
	}

}
