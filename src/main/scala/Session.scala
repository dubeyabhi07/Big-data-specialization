package sessioninit

import org.apache.spark.sql.SparkSession

object Session {

  def startSparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("hands-on-spark")
      .master("local")
      .getOrCreate()
    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("ERROR")
    sparkSession
  }

}
