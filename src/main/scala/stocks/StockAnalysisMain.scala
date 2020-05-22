package stocks

import sessioninit.Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD


object StockAnalysisMain {
  def main(args: Array[String]): Unit = {


    val sparkSession = Session.startSparkSession;
    import sparkSession.sqlContext.implicits._

    val stocksToBeLoaded = Array("HDFC", "ICICI", "cipla", "HUL", "sunpharma", "DLF", "indiaBulls",
      "reliance", "ONGC", "tataMotor", "hero", "symphony", "whirlpool", "donear", "raymond");

    var stocksDataFramesMap = Utility.getStocksDataFrames(sparkSession, stocksToBeLoaded);
    Utility.createStocksTempViews(stocksDataFramesMap);

    StockAnalysisSQL.runSqlQueries(sparkSession.sqlContext, stocksToBeLoaded);
    StockAnalysisDF.runQueriesWithDataFrameAPIs(stocksDataFramesMap);

  }

}

