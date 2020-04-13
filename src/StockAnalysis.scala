import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

case class Stock(
  stockName:     String,
  dt:            String,
  openPrice:     Double,
  highPrice:     Double,
  lowPrice:      Double,
  closePrice:    Double,
  adjClosePrice: Double,
  volume:        Double)

object StockAnalysis {

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

  def main(args: Array[String]): Unit = {

    val sparkSess = SparkSession.builder()
      .appName("twitter-trending-hashtags")
      .master("local")
      .getOrCreate()
    val sc = sparkSess.sparkContext
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //HDFC bank
    val HDFC = parseRDD(sc.textFile("resources/stocks/HDFCBANK.NS.csv"), "HDFC").toDF.na.drop()
    HDFC.createTempView("HDFCView")

    //ICICI bank
    val ICICI = parseRDD(sc.textFile("resources/stocks/ICICIBANK.NS.csv"), "ICICI").toDF.na.drop()
    ICICI.createTempView("ICICIView")

    //Cipla
    val cipla = parseRDD(sc.textFile("resources/stocks/CIPLA.NS.csv"), "cipla").toDF.na.drop()
    cipla.createTempView("ciplaView")

    //Hindustan Uniliver
    val HUL = parseRDD(sc.textFile("resources/stocks/HINDUNILVR.NS.csv"), "HUL").toDF.na.drop()
    HUL.createTempView("HULView")

    //Sunpharma
    val sunpharma = parseRDD(sc.textFile("resources/stocks/Sunpharma.NS.csv"), "sunpharma").toDF.na.drop()
    sunpharma.createTempView("sunpharmaView")

    //DLF bank
    val DLF = parseRDD(sc.textFile("resources/stocks/DLF.NS.csv"), "DLF").toDF.na.drop()
    DLF.createTempView("DLFView")

    //India bulls real-estate
    val indiaBulls = parseRDD(sc.textFile("resources/stocks/IBREALEST.NS.csv"), "indiaBulls").toDF.na.drop()
    indiaBulls.createTempView("indiaBullsView")

    //Reliance
    val reliance = parseRDD(sc.textFile("resources/stocks/RELIANCE.NS.csv"), "reliance").toDF.na.drop()
    reliance.createTempView("relianceView")

    //ONGC
    val ONGC = parseRDD(sc.textFile("resources/stocks/ONGC.NS.csv"), "ONGC").toDF.na.drop()
    ONGC.createTempView("ONGCView")

    //TATA motors
    val tataMotors = parseRDD(sc.textFile("resources/stocks/TATAMOTORS.NS.csv"), "tataMotors").toDF.na.drop()
    tataMotors.createTempView("tataMotorsView")

    //Hero motocorp
    val hero = parseRDD(sc.textFile("resources/stocks/HEROMOTOCO.NS.csv"), "hero").toDF.na.drop()
    hero.createTempView("heroView")

    //Symphony
    val symphony = parseRDD(sc.textFile("resources/stocks/SYMPHONY.NS.csv"), "symphony").toDF.na.drop()
    symphony.createTempView("symphonyView")

    //Whirlpool
    val whirlpool = parseRDD(sc.textFile("resources/stocks/WHIRLPOOL.NS.csv"), "whirlpool").toDF.na.drop()
    whirlpool.createTempView("whirlpoolView")

    //Donear
    val donear = parseRDD(sc.textFile("resources/stocks/DONEAR.NS.csv"), "donear").toDF.na.drop()
    donear.createTempView("donearView")

    //Raymond
    val raymond = parseRDD(sc.textFile("resources/stocks/RAYMOND.NS.csv"), "raymond").toDF.na.drop()
    raymond.createTempView("raymondView")

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

    var profits = sqlContext.sql(createQueryForMaxProfit(
      Array("HDFC", "ICICI", "cipla", "HUL", "sunpharma", "DLF", "indiaBulls", "reliance",
        "ONGC", "tataMotors", "hero", "symphony", "whirlpool", "donear", "raymond")));

    println("The profit earned per unit stock in 2019 is in order : ");
    profits.show
    profits.createTempView("profits");

    var profitPercentage = sqlContext.sql(createQueryforMaxPercentageProfit(
      Array("HDFC", "ICICI", "cipla", "HUL", "sunpharma", "DLF", "indiaBulls", "reliance",
        "ONGC", "tataMotors", "hero", "symphony", "whirlpool", "donear", "raymond")));

    profitPercentage.show

  }

}

