package stocks

import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

case class Stock(
  stockName:     String,
  dt:            String,
  openPrice:     Double,
  highPrice:     Double,
  lowPrice:      Double,
  closePrice:    Double,
  adjClosePrice: Double,
  volume:        Double)

class Utility {

}

object Utility {

  val props = ConfigFactory.load("application.properties");

  def parseStock(inputRecord: String, stockName: String): Stock = {
    val coloumn = inputRecord.split(",")
    Stock(
      stockName,
      coloumn(0),
      coloumn(1).toDouble,
      coloumn(2).toDouble,
      coloumn(3).toDouble,
      coloumn(4).toDouble,
      coloumn(5).toDouble,
      coloumn(6).toDouble)
  }

  def parseRDD(rdd: RDD[String], stockName: String): RDD[Stock] = {
    val header = rdd.first
    rdd.filter((data) => { data(0) != header(0) && !data.contains("null") })
      .map(data => parseStock(data, stockName))
  }

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

  def getStocksDataFrames(sparkSession: SparkSession, sqlContext: SQLContext,stocksToBeLoaded : Array[String]): Map[String, DataFrame] = {
    import sqlContext.implicits._
    var dataFrameMap: Map[String, DataFrame] = Map()
    for (stock <- stocksToBeLoaded) {
      dataFrameMap += (stock ->
        parseRDD(sparkSession.sparkContext.textFile(props.getString(stock)), stock).toDF.na.drop())
    } 
    dataFrameMap;
  }
  
  def createStocksTempViews(dataFrameMap : Map[String, DataFrame]) ={
    dataFrameMap.foreach(pair=>pair._2.createTempView(pair._1+"View"))
  }
}

