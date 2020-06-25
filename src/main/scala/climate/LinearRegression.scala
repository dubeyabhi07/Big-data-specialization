package climate

import sessioninit.Session
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, Encoders, SQLContext, SparkSession}
import scalafx.application.JFXApp
import org.apache.spark.sql.functions._

import scala.util.Random


object LinearRegression extends App {

  val sparkSession = Session.startSparkSession;

  import sparkSession.sqlContext.implicits._

  val stationClustersByLatLon = Utility.createStationClusterByLatLong(sparkSession,10)
  val observedData = Utility.returnNOAAObservatoryData(sparkSession)

  val rand = Random.nextInt(10)
  val randomCluster = stationClustersByLatLon.filter(col("cluster") === rand)
  println("no. of stations in cluster-" + rand + " : " + randomCluster.count())


  /*
   * 1. Comparing results of two linear regression models on observation data for "TMAX" feature.
   * Description :  A geographical cluster is selected at random that has many observation
   * stations. We try to train 2 (y=ax+bx^2,y = asin(x)+bcos(x)) linear regression models
   * and compare them against actual observations.
   *
   * */

  val maxTempData = observedData.filter(col("measure") === "TMAX")
    .join(randomCluster, "sid").cache()
  println("TMAX data related to cluster-" + rand + " : ")
  println("DataSize : " + maxTempData.count())
  maxTempData.show();


  val splitData = maxTempData
    .withColumn("doy", dayofyear(col("date")))
    .withColumn("doySin", sin((col("doy") / 365) * 2 * math.Pi))
    .withColumn("doyCos", cos((col("doy") / 365) * 2 * math.Pi))
    .withColumn("doySquare", col("doy") * col("doy"))
    .select(col("sid"), col("date"), col("doy"),
      col("value"), col("doySquare"),
      col("doyCos"), col("doySin"))
    .randomSplit(Array[Double](0.8, 0.2))
  val testData = splitData(1).cache()
  val trainData = splitData(0).cache()

  val lr1DataSchema = new VectorAssembler()
    .setInputCols(Array("doy", "doySquare"))
    .setOutputCol("lr1Input")

  val lr2DataSchema = new VectorAssembler()
    .setInputCols(Array("doySin", "doyCos"))
    .setOutputCol("lr2Input")

  val lr1Algo = new LinearRegression()
    .setFeaturesCol("lr1Input")
    .setLabelCol("value")
    .setMaxIter(100)
    .setPredictionCol("predictedMaxTemp1")

  val lr2Algo = new LinearRegression()
    .setFeaturesCol("lr2Input")
    .setLabelCol("value")
    .setMaxIter(100)
    .setPredictionCol("predictedMaxTemp2")

  val lrModel1 = lr1Algo.fit(lr1DataSchema.transform(trainData))
  val lrModel2 = lr2Algo.fit(lr2DataSchema.transform(trainData))

  var lr1Result = lrModel1.transform(lr1DataSchema.transform(testData))
  var lr2Result = lrModel2.transform(lr2DataSchema.transform(testData))

  lr1Result = lr1Result
    .withColumn("difference",abs(col("value")-col("predictedMaxTemp1")))
    .select('value,'predictedMaxTemp1,'difference)
  lr2Result = lr2Result
    .withColumn("difference",abs(col("value")-col("predictedMaxTemp2")))
    .select('value,'predictedMaxTemp2,'difference)

  println("glimpse of y = ax+bx^2 model : ")
  lr1Result.show()
  println("glimpse of y = asinx(x)+bcos(x) model : ")
  lr2Result.show()

  println("Mean Error in y=ax+bx^2 Model is :"+
    lr1Result.agg(avg('difference).as("data")).select(col("data")).first()
  )
  println("Mean Error in y=asin(x)+bcos(x) Model is :"+
    lr2Result.agg(avg('difference).as("data")).select(col("data")).first()
  )

}
