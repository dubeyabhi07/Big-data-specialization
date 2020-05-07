package climate

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}

case class Station(
                    sid: String,
                    lat: Double,
                    lon: Double,
                    elev: Double,
                    name: String
                  )

case class NOAAData(
                     sid: String,
                     date: java.sql.Date,
                     measure: String,
                     value: Double
                   )

object Utility {


  def startSparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("twitter-trending-hashtags")
      .master("local")
      .getOrCreate()
    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sparkContext)
    (sparkSession, sqlContext)
  }

  def createStationClusterByLatLong(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val stations = sparkSession.read.textFile("src/main/resources/NOAA/ghcnd-stations.txt")
      .map(line => {
        val id = line.substring(0, 11)
        val lat = line.substring(12, 20).trim.toDouble
        val lon = line.substring(21, 30).trim.toDouble
        val elev = line.substring(31, 37).trim.toDouble
        val name = line.substring(41).trim
        Station(id, lat, lon, elev, name)
      }
      )

    val stationsWithLoc = new VectorAssembler()
      .setInputCols(Array("lat", "lon"))
      .setOutputCol("location")
      .transform(stations)


    val kMeansAlgo = new KMeans()
      .setK(10)
      .setFeaturesCol("location")
      .setPredictionCol("cluster")

    val stationClusterModel = kMeansAlgo.fit(stationsWithLoc)

    val stationsWithClusters = stationClusterModel.transform(stationsWithLoc)
    println("Observation stations clustered according to geography as :")
    println("Datasize is : "+stationsWithClusters.count())
    stationsWithClusters.show()
    stationsWithClusters
  }

  def returnNOAAObservatoryData(sparkSession: SparkSession, fraction: Int): DataFrame = {
    val observationData = sparkSession.read.schema(Encoders.product[NOAAData].schema).
      option("dateFormat", "yyyyMMdd").csv("src/main/resources/NOAA/1900.csv")

    println("NOAA Observatory Data :")
    println("Datasize is : "+observationData.count())
    observationData.show()
    observationData
  }

}
