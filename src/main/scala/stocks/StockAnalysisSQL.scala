package stocks

import org.apache.spark.sql.SQLContext

object StockAnalysisSQL {

  def runSqlQueries(sqlContext: SQLContext, stocksToBeLoaded: Array[String]): Unit = {

    //##################################### Basic queries ###################################################

    /*
     * 1. Most Volatile stocks for intra-day in 2019 (descending order)
     * Description :  stock that remained least stable.
     * average of absolute difference between daily high and low price.
     *
     * */

    def createQueryForMaxIntraDayVolatility(stocks: Array[String]): String = {
      var query: String = "";
      for (stock <- stocks) {
        if (stock != stocks(0)) {
          query += "UNION";
        }
        var subQuery = "SELECT stock, AVG(dailyDiffRatio) AS intraDayAvgVolatilityPercent " +
          " FROM ( SELECT stockName AS stock, 100*abs(highPrice - lowPrice)/openPrice AS dailyDiffRatio" +
          " FROM " + stock + "View )" +
          " GROUP BY stock";
        println(subQuery);
        query += "( " + subQuery + " )";
      }
      query += "ORDER BY intraDayAvgVolatilityPercent DESC"
      return query;
    }

    var intraDayAvgVolatility = sqlContext.sql(createQueryForMaxIntraDayVolatility(stocksToBeLoaded));
    println("The most volatile stocks (intra-day) in 2019 are in order : ");
    intraDayAvgVolatility.show

    /*
     * 2. Most Volatile stocks for inter-day in 2019 (descending order)
     * Description :  stock that remained least stable overnight.
     * average of absolute difference between opening price and closing price of previous day.
     *
     * */

    def createQueryForMaxInterDayVolatility(stocks: Array[String]): String = {
      var query: String = "";
      for (stock <- stocks) {
        if (stock != stocks(0)) {
          query += "UNION";
        }
        var subQuery = "SELECT stock, AVG(interdayDiffRatio) AS interDayAvgVolatilityPercent " +
          " FROM ( SELECT stockName AS stock," +
          " 100*abs(closePrice - (lead(openPrice) over (order by dt)))/closePrice AS interDayDiffRatio" +
          " FROM " + stock + "View )" +
          " GROUP BY stock";
        println(subQuery);
        query += "( " + subQuery + " )";
      }
      query += "ORDER BY interDayAvgVolatilityPercent DESC"
      return query;
    }

    var interDayAvgVolatility = sqlContext.sql(createQueryForMaxInterDayVolatility(stocksToBeLoaded));
    println("The volatile stocks(inter-day) in 2019 are in order : ");
    interDayAvgVolatility.show

    /*
     * 3. Most profit earned per unit stock in 2019 (descending order)
     * Description :  which stock would have given most profit per unit in year 2019,
     *  assuming it was bought on first day and sold on last day of 2019.
     *
     * */

    def createQueryForMaxProfit(stocks: Array[String]): String = {
      var query: String = "";
      for (stock <- stocks) {
        if (stock != stocks(0)) {
          query += "UNION";
        }
        var subQuery = "SELECT stock, SUM(price) AS profit, SUM(basePrice) AS basePrice " +
          " FROM ( SELECT stockName AS stock, closePrice as price, 0 as basePrice FROM " + stock + "View WHERE dt = '2019-12-31'" +
          " UNION" +
          " SELECT stockName AS stock, openPrice*-1 as price, openPrice as basePrice  FROM " + stock + "View WHERE dt = '2019-01-01' )" +
          " GROUP BY stock";
        println(subQuery);
        query += "( " + subQuery + " )";
      }
      query += "ORDER BY profit DESC"
      return query;
    }

    var profits = sqlContext.sql(createQueryForMaxProfit(stocksToBeLoaded));
    println("The profit earned per unit stock in 2019 is in order : ");
    profits.show

    /*
     * 4. Most profitable stocks in 2019 (descending order)
     * Description :  which stock would have given most profit in year 2019,
     *  assuming it was bought on first day and sold on last day of 2019.
     *
     * */

    profits.createTempView("profits");

    def createQueryforMaxPercentageProfit(): String = {
      var query: String = "";

      query = "SELECT profits.stock, profits.profit, profits.basePrice," +
        " (100*(profits.profit / profits.basePrice)) AS profitPercent FROM profits "
      query += "ORDER BY profitPercent DESC"

      return query;
    }

    var profitPercentage = sqlContext.sql(createQueryforMaxPercentageProfit());
    println("The % profit earned per unit stock in 2019 is in order : ");
    profitPercentage.show
  }
}