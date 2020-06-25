package climate

import sessioninit.Session
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

case class ClusterData(num: Int, lat: Double, lon: Double, latstd: Double, lonstd: Double,
                       tmax: Double, tmin: Double, tmaxstd: Double, tminstd: Double, precip: Double,
                       tmaxSeasonalVar: Double, tminSeasonalVar: Double)

object ClimateZones {
  val sparkSession = Session.startSparkSession;

  import sparkSession.sqlContext.implicits._


  def main(args: Array[String]): Unit = {
    //for better results pass numCluster >= 1000. Make sure enough compute power is available.
    var numCluster = 10
    val stationClustersByLatLon = Utility.createStationClusterByLatLong(sparkSession, numCluster)
    val observedData = Utility.returnNOAAObservatoryData(sparkSession)

    /*
    * Firstly geographical zones are created, then their properties such as maximum temperature,
    * minimum temperature, seasonal variation in temperature, precipitation, standard deviation of coordinates etc
    * are taken into account and then climate zones are created. These zones are expected to have similar climatic conditions.
    * */


    val joinedData = observedData.join(stationClustersByLatLon , "sid").cache()
    val clusterData = (0 until numCluster).par.flatMap(i => calcClusterData(joinedData, i)).seq
    val clusterDS = sparkSession.createDataset(clusterData)

    clusterDS.show(5)

    val clusterDataVA= new VectorAssembler()
      .setInputCols(Array("lat", "lon","latstd","lonstd","tmin","tmax","tmaxstd","tminstd","precip",
      "tmaxSeasonalVar","tminSeasonalVar"))
      .setOutputCol("zoneInput")
      .transform(clusterDS)


    val kMeansAlgo = new KMeans()
      // diving into 5 climate zones
      .setK(5)
      .setFeaturesCol("zoneInput")
      .setPredictionCol("climateZone")

    val climateZoneModel = kMeansAlgo.fit(clusterDataVA)

    val climateZones = climateZoneModel.transform(clusterDataVA)
    println("climate zones predicted as :")
    println("Datasize is : "+climateZones.count())
    climateZones.show()

    //TODO : add visualization
  }


  def calcSeasonalVar(df: DataFrame): Double = {
    val withDOYinfo = df.withColumn("doy", dayofyear('date)).
      withColumn("doySin", sin('doy / 365 * 2 * math.Pi)).
      withColumn("doyCos", cos('doy / 365 * 2 * math.Pi))
    val linearRegData = new VectorAssembler().setInputCols(Array("doySin", "doyCos")).
      setOutputCol("doyTrig").transform(withDOYinfo).cache()
    val linearReg = new LinearRegression().setFeaturesCol("doyTrig").setLabelCol("value").
      setMaxIter(10).setPredictionCol("predictedValue")
    val linearRegModel = linearReg.fit(linearRegData)
    math.sqrt(linearRegModel.coefficients(0) * linearRegModel.coefficients(0) +
      linearRegModel.coefficients(1) * linearRegModel.coefficients(1))
  }

  def calcClusterData(df: DataFrame, cluster: Int): Option[ClusterData] = {
    val filteredData = df.filter('cluster === cluster).cache()
    val tmaxs = filteredData.filter('measure === "TMAX").cache()
    val tmins = filteredData.filter('measure === "TMIN").cache()
    val precips = filteredData.filter('measure === "PRCP").cache()
    val cd = if (tmaxs.count() < 20 || tmins.count() < 20 || precips.count() < 3) None else {
      val latData = filteredData.agg(avg('lat) as "lat", stddev('lat) as "latstd")
      val lat = latData.select('lat).as[Double].first()
      val latstd = latData.select('latstd).as[Double].first()
      val lonData = filteredData.agg(avg('lon) as "lon", stddev('lon) as "lonstd")
      val lon = lonData.select('lon).as[Double].first()
      val lonstd = lonData.select('lonstd).as[Double].first()
      val tmaxData = tmaxs.agg(avg('value) as "tmax", stddev('value) as "tmaxstd")
      val tmax = tmaxData.select('tmax).as[Double].first()
      val tmaxstd = tmaxData.select('tmaxstd).as[Double].first()
      val tminData = tmins.agg(avg('value) as "tmin", stddev('value) as "tminstd")
      val tmin = tminData.select('tmin).as[Double].first()
      val tminstd = tminData.select('tminstd).as[Double].first()
      val precip = precips.agg(avg('value) as "precip").select('precip).as[Double].first()
      val tmaxSeasonalVar = calcSeasonalVar(tmaxs)
      val tminSeasonalVar = calcSeasonalVar(tmins)
      Some(ClusterData(cluster, lat, lon, latstd, lonstd, tmax, tmin, tmaxstd, tminstd,
        precip, tmaxSeasonalVar, tminSeasonalVar))
    }
    filteredData.unpersist()
    tmaxs.unpersist()
    tmins.unpersist()
    precips.unpersist()
    cd
  }
}
