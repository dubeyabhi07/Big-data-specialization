package stocks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

object StockAnalysisMain {
  def main(args: Array[String]): Unit = {

    val (sparkSession, sqlContext) = Utility.startSparkSession;
    import sqlContext.implicits._

    val stocksToBeLoaded = Array("HDFC", "ICICI", "cipla", "HUL", "sunpharma", "DLF", "indiaBulls",
      "reliance", "ONGC", "tataMotor", "hero", "symphony", "whirlpool", "donear", "raymond");

    var stocksDataFramesMap = Utility.getStocksDataFrames(sparkSession, sqlContext, stocksToBeLoaded);
    Utility.createStocksTempViews(stocksDataFramesMap);

    StockAnalysisSQL.runSqlQueries(sqlContext, stocksToBeLoaded);
    StockAnalysisDF.runQueriesWithDataFrameAPIs(stocksDataFramesMap);

  }

}

