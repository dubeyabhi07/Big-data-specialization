package common

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Session {
  def main(args: Array[String]): Unit = {

    /**
     * following method to get sparkContext is obsolete, hence commented :
     *
     * val sparkConf = new SparkConf()
     * sparkConf.setMaster("local")
     * sparkConf.setAppName("Analysis-with-Spark")
     * val sparkContext = new SparkContext(sparkConf)
     */

    val sparkSession = SparkSession.builder()
      .appName("Analysis-with-Spark")
      .master("local")
      .getOrCreate()

  }
}