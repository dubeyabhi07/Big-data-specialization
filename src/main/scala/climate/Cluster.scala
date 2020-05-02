package climate

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SQLContext, SparkSession}
import scalafx.application.JFXApp
import org.apache.spark.sql.Encoders

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


object Cluster extends JFXApp {

  //def main(args: Array[String]): Unit = {
  val sparkSession = SparkSession.builder()
    .appName("nse-stocks-analysis")
    .master("local[*]")
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("ERROR")
  val sqlContext = new SQLContext(sparkContext)

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

  stationsWithLoc.show()

  val kMeansAlgo = new KMeans()
    .setK(1000)
    .setFeaturesCol("location")
    .setPredictionCol("cluster")

  val stationClusterModel = kMeansAlgo.fit(stationsWithLoc)

  val stationsWithClusters = stationClusterModel.transform(stationsWithLoc)
  val data = stationsWithClusters
  stationsWithClusters.show()


  val data2019 = sparkSession.read.schema(Encoders.product[NOAAData].schema).
    option("dateFormat", "yyyyMMdd").csv("src/main/resources/NOAA/2019.csv")
    .sample(0.1)

  val joinedData = data2019.join(stationsWithClusters, "sid")

  joinedData.show()
}
