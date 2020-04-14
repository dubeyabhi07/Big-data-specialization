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
    
    
    
    
    

    //##################################### Basic queries ###################################################

    /*
     * 1. Most profitable stocks in 2019 (descending order)
     * Description :  which stock would have given most profit in year 2019,
     *  assuming it was bought on first day and sold on last day of 2019.
     *
     * */

    def createQueryForMaxProfit(stocks: Array[String]): String = {
      var query: String = "";
      for (stock <- stocks) {
        if (stock != stocks(0)) {
          query += "UNION";
        }
        var subQuery = "SELECT stock, SUM(price) AS profit " +
          " FROM ( SELECT stockName AS stock, closePrice as price FROM " + stock + "View WHERE dt = '2019-12-31'" +
          " UNION" +
          " SELECT stockName AS stock, openPrice*-1 as price FROM " + stock + "View WHERE dt = '2019-01-01' )" +
          " GROUP BY stock";
        println(subQuery);
        query += "( " + subQuery + " )";
      }
      query += "ORDER BY profit DESC"
      return query;
    }

    def createQueryforMaxPercentageProfit(stocks: Array[String]): String = {
      var query: String = "";
      for (stock <- stocks) {
        if (stock != stocks(0)) {
          query += "UNION";
        }
        var subQuery = " SELECT stockName AS stock, openPrice FROM " + stock + "View WHERE dt = '2019-01-01'"
        println(subQuery);
        query += "( " + subQuery + " )";
      }

      query = "SELECT profits.stock, profits.profit, data.openPrice," +
        " (100*(profits.profit / data.openPrice)) AS profitPercent FROM (" + query + ") AS data" +
        " JOIN profits ON data.stock = profits.stock ";

      query += "ORDER BY profitPercent DESC"

      return query;
    }

    var profits = sqlContext.sql(createQueryForMaxProfit(stocksToBeLoaded));
    println("The profit earned per unit stock in 2019 is in order : ");
    profits.show

    profits.createTempView("profits");

    var profitPercentage = sqlContext.sql(createQueryforMaxPercentageProfit(stocksToBeLoaded));
    profitPercentage.show
    
    
    
    /*
     * 2. Most Volatile stocks in 2019 (descending order)
     * Description :  stock that remained least stable.
     *  
     * */

  }

}

