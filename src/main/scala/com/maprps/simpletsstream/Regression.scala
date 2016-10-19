package com.maprps.simpletsstream

import org.apache.log4j.{Logger, Level}
//core and SparkSQL
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructType, StructField, StringType, TimestampType}
// ML Feature Creation, Tuning, Models, and Model Evaluation
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression, GBTRegressor}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics

object Regression{
        /*
	val storeEncoder = new OneHotEncoder()
	.setInputCol("Store")
	.setOutputCol("StoreVec")
	val stateHolidayIndexer = new StringIndexer()
	.setInputCol("StateHoliday")
	.setOutputCol("StateHolidayIndex")
	val stateHolidayEncoder = new OneHotEncoder()
	.setInputCol("StateHolidayIndex")
	.setOutputCol("StateHolidayVec")
	val schoolHolidayIndexer = new StringIndexer()
	.setInputCol("SchoolHoliday")
	.setOutputCol("SchoolHolidayIndex")
	val schoolHolidayEncoder = new OneHotEncoder()
	.setInputCol("SchoolHolidayIndex")
	.setOutputCol("SchoolHolidayVec")
	val dayOfMonthEncoder = new OneHotEncoder()
	.setInputCol("DayOfMonth")
	.setOutputCol("DayOfMonthVec")
	val dayOfWeekEncoder = new OneHotEncoder()
	.setInputCol("DayOfWeek")
	.setOutputCol("DayOfWeekVec")
	val monthEncoder = new OneHotEncoder()
	.setInputCol("Month")
	.setOutputCol("MonthVec")
	val quarterEncoder = new OneHotEncoder()
	.setInputCol("Quarter")
	.setOutputCol("QuarterVec")
	val yearEncoder = new OneHotEncoder()
	.setInputCol("Year")
	.setOutputCol("YearVec")
	val storeTypeIndexer = new StringIndexer()
	.setInputCol("StoreType")
	.setOutputCol("StoreTypeIndex")
	val storeTypeEncoder = new OneHotEncoder()
	.setInputCol("StoreTypeIndex")
	.setOutputCol("StoreTypeVec")
	val assortmentIndexer = new StringIndexer()
	.setInputCol("Assortment")
	.setOutputCol("AssortmentIndex")
	val assortmentEncoder = new OneHotEncoder()
	.setInputCol("AssortmentIndex")
	.setOutputCol("AssortmentVec")
	val competitionOpenSinceMonthEncoder = new OneHotEncoder()
	.setInputCol("CompetitionOpenSinceMonth")
	.setOutputCol("CompetitionOpenSinceMonthVec")
	val competitionOpenSinceYearEncoder = new OneHotEncoder()
	.setInputCol("CompetitionOpenSinceYear")
	.setOutputCol("CompetitionOpenSinceYearVec")
	val promoIntervalIndexer = new StringIndexer()
	.setInputCol("PromoInterval")
	.setOutputCol("PromoIntervalIndex")
	val promoIntervalEncoder = new OneHotEncoder()
	.setInputCol("PromoIntervalIndex")
	.setOutputCol("PromoIntervalVec")

	val assembler = new VectorAssembler()
	.setInputCols(Array("StoreVec", "Open", "Promo", "StateHolidayVec", "SchoolHolidayVec",
		"DayOfWeekVec", "DayOfMonthVec", // "MonthVec", "QuarterVec", "YearVec",
		"StoreTypeVec", "AssortmentVec", "CompetitionDistance","CompetitionOpenSinceMonthVec",
		"CompetitionOpenSinceYearVec",  // "Promo2", "Promo2SinceWeek", "Promo2SinceYear",
		"PromoIntervalVec", "monthlyAvgSales", "monthlyMedSales"))
	.setOutputCol("features")

	def preprocessRFPipeline() : TrainValidationSplit = {
		val rf = new RandomForestRegressor()
		val paramGrid = new ParamGridBuilder()
			.addGrid(rf.minInstancesPerNode, Array(5))
			.addGrid(rf.maxDepth, Array(8))
			.addGrid(rf.numTrees, Array(20))
			.addGrid(rf.subsamplingRate, Array(0.9))
			.build()

		val pipeline = new Pipeline()
			.setStages(Array(storeEncoder, stateHolidayIndexer, schoolHolidayIndexer,
			stateHolidayEncoder, schoolHolidayEncoder, dayOfWeekEncoder, dayOfMonthEncoder,
			storeTypeIndexer, storeTypeEncoder,
			assortmentIndexer, assortmentEncoder, competitionOpenSinceMonthEncoder,
			competitionOpenSinceYearEncoder, promoIntervalIndexer, promoIntervalEncoder,
			assembler, rf))

		val cv = new TrainValidationSplit()
			.setEstimator(pipeline)
			.setEvaluator(new RegressionEvaluator)
			.setEstimatorParamMaps(paramGrid)
			.setTrainRatio(0.7)
		cv
	}

	def fitModel(cv: TrainValidationSplit, data: DataFrame): TrainValidationSplitModel = {

		val Array(train, test) = data.randomSplit(Array(0.9, 0.1), seed = 2016)
    	printf("Fitting data")
	    train.repartition(24).cache()
	    // test.cache()
	    val model = cv.fit(train)
	    printf("Now performing test on hold out set")
	    val holdout = model.transform(test).select("prediction","label")

        // have to do a type conversion for RegressionMetrics
	val rm = new RegressionMetrics(holdout.rdd.map(x =>
		(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

	println("-------Test Metrics----------")
	println("--Test Explained Variance:---")
	println(rm.explainedVariance)
	println("------Test R^2 Coef:---------")
	println(rm.r2)
	println("------Test MSE:--------------")
	println(rm.meanSquaredError)
	println("------Test RMSE:-------------")
	println(rm.rootMeanSquaredError)

	model
	}

	def trainModel(sqlContext:HiveContext):TrainValidationSplitModel = {
	val data = readTrain(sqlContext)
	// The linear Regression Pipeline
	val rfCv = preprocessRFPipeline()
	println("-----evaluating random forest---------")
	val rfModel = fitModel(rfCv, data)
	rfModel
	}

	def main(args:Array[String]) = {
	val WINDOW_LENGTH = new Duration(28 * 1000)
	val SLIDE_INTERVAL = new Duration(1 * 1000)
	val name = "TS Regression Application"
	println(s"Starting up $name")

	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	val conf = new SparkConf().setAppName(name)
	val sc = new SparkContext(conf)
	val sqlContext = new HiveContext(sc)
	// sc.setLogLevel("INFO")
	println("--------Set Up Complete-----------")
	val rfModel = trainModel(sqlContext)

	println("Generating TS predictions")

	val sscFeature = new StreamingContext(sc, Seconds(1))
	val lines = ssc.socketTextStream("localhost", 9999)

	//val lines = sc.textfile("rossmann/testSplit.csv")
	val streamSchema = StructType(Seq(StructField("Store", StringType, true),
	StructField("DayOfWeek", StringType, true), StructField("Date", StringType, true),
	StructField("Sales", String, true), StructField("Customers", String, true),
	StructField("Open", String, true), StructField("Promo", String, true),
	StructField(" StateHoliday", String, true), StructField("SchoolHoliday", String, true)))

	val fieldStream = lines.flatMap(_.split(",")).cache()
	val windowTestStream = fieldStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)
	windowTestStream.foreachRDD(block => {
		if (block.count()==0){
		println("No test data received in this time interval")
		} else {
		val blockDataframe = sqlContext.createDataFrame(block, fieldSchema)
		blockDataframe.registerTempTable("window_test")
		val monthlyTest = sqlContext.sql("select avg(double(Sales)) monthlyAvgSales, percentile(int(Sales), 0.5) monthlyMedSales, date_add(to_date(max(Date)), 30) Date, Store from windows_test group by Store")
		monthlyTest.registerTempTable("monthly_test")
		val testFull = sqlContext.sql(""" Select
			double(a.Sales) label, double(a.Store) Store, double(a.Open) Open, double(a.Promo) Promo, double(a.StateHoliday) StateHoliday, double(a.SchoolHoliday) SchoolHoliday, double(dayofmonth(a.Date))  DayOfMonth, double(a.DayofWeek) DayOfWeek, double(month(a.Date)) Month, double(quarter(a.Date))  Quarter, double(year(a.Date)) Year, b.StoreType, b.Assortment, double(b.CompetitionDistance)  CompetitionDistance, double(b.CompetitionOpenSinceMonth) CompetitionOpenSinceMonth, double(b.  CompetitionOpenSinceYear) CompetitionOpenSinceYear, double(b.Promo2) as Promo2, double(b.Promo2SinceWeek) as Promo2SinceWeek, double(b.Promo2SinceYear) as Promo2SinceYear, b.PromoInterval,  to_date(a.Date) Date, c.monthlyAvgSales, c.monthlyMedSales
			from monthly_test t1
			left join train_full t2
			on t1.Date = t2.Date and t1.Store = t2.Store""")
		val rfOut = rfModel.transform(testFull)
			.withColumnRenamed("prediction","Sales")
			.select("Sales", "Date", "Store")
		savePrediction(rfOut)
		}
	})
	}
*/
}