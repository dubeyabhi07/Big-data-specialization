package stocks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

class Utility {

}

object Utility {

  def startSparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("twitter-trending-hashtags")
      .master("local")
      .getOrCreate()
    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    (sparkSession, sparkContext, sqlContext)
  }
}

