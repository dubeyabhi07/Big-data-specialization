package stocks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.RelationalGroupedDataset

object StockAnalysisDF {
  def runQueriesWithDataFrameAPIs(dataFrameMap: Map[String, DataFrame]) = {

    val windowSpecMonth = Window.partitionBy("month");
    /*
     * 1. Most profitable stock-month combination stock in 2019 (descending order)
     * Description :  which stock would have given most profit in any month of year 2019,
     *  assuming it was bought on first day of that month and sold on last day of that month in 2019.
     *
     * */

    var derivedMap: Map[String, DataFrame] = dataFrameMap.mapValues((df) =>
      df.withColumn("month", col("dt").substr(6, 2))
        //if this orderBy is added to windowSpec then columns with agg will have cumulative results
        .withColumn("row", row_number.over(windowSpecMonth.orderBy(col("dt").desc)))
        .withColumn("maxRow", max(col("row")).over(windowSpecMonth))
        .withColumn("relevantPrice", when(col("row") === 1, col("closePrice"))
          .when(col("row") === col("maxRow"), col("openPrice") * (-1))
          .otherwise(0))
        .withColumn("referencePrice", when(col("row") === col("maxRow"), col("openPrice"))
          .otherwise(0))
        .select(col("stockName"), col("month"), col("maxRow"),
          col("row"), col("relevantPrice"), col("referencePrice"))
        .where(col("row") === 1 || col("row") === col("maxRow"))
        .drop(col("row"))
        .drop(col("maxRow"))
        .groupBy(col("month"), col("stockName").alias("stock"))
        .agg(
          sum("relevantPrice").as("maxProfitPerUnit"),
          sum("referencePrice").as("referencePrice"))
        .select(col("month"), col("stock"), col("referenceprice").as("baseOpeningPrice"), col("maxProfitPerUnit"),
          ((col("maxProfitPerUnit") * 100) / col("referencePrice")).as("maxProfitPercent")))
    println("Most profitable stock-month combination stock in 2019 (descending order) : ")
    var result1 = derivedMap.reduce((v1, v2) => (v1._1, v1._2.union(v2._2)))
    result1._2.orderBy(col("maxProfitPercent").desc).show(100)

    /*
     * 2. Most profitable stock for each month in 2019
     * Description :  which stock would have given most profit in each month of year 2019,
     *  assuming it was bought on first day of that month and sold on last day of that month in 2019.
     *
     * */

    var result2 = result1._2
      //groupBy operation here makes it difficult to retrieve the name of stock
      .withColumn("row", row_number.over(windowSpecMonth.orderBy(col("maxProfitPercent").desc)))
      .where(col("row") === 1)
      .drop(col("row"))
      .orderBy(col("month"))
    println("Most profitable stock for each month in 2019 : ")
    result2.show()


  }
}