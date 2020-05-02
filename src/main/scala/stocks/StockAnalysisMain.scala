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
    //StockAnalysisDF.runQueriesWithDataFrameAPIs(stocksDataFramesMap);

    //    var m: scala.collection.mutable.ArrayBuffer[(String, Int)] = scala.collection.mutable.ArrayBuffer()
    //    m += ("kk" -> 67)
    //    m += ("kk" -> 12)
    //    m += ("abcd" -> 100)
    //    m += ("nyc" -> 12)
    //    m.foreach(println)
    //    var rdd = sparkSession.sparkContext.parallelize(m, 2)
    //    var rdd1 = rdd.reduceByKey(_ + _)
    //    rdd1.foreach(println)

    //    val listRDD = sparkSession.sparkContext.parallelize(List(1, 2, 3, 4), 2)
    //    listRDD.collect()
    //    listRDD.aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    //        var rdd2 = sparkSession.sparkContext.parallelize(List(("s1" -> 1), ("s2" -> 2), ("s3" -> 3), ("s4" -> 4)), 2);
    //        var rdd2Result = rdd2.aggregate(("s0" -> 0))((acc, record) =>
    //          {
    //            var temp = new Tuple2(acc._1 + record._1,acc._2 + record._2)
    //            temp
    //          }, (acc1, acc2) => {
    //          var temAcc: (String, Int) = new Tuple2(acc1._1 + acc2._1, acc1._2 + acc2._2)
    //          temAcc
    //        })
    //        println(rdd2Result);

    //        var rdd22 = sparkSession.sparkContext.parallelize(List(("s1" -> 1), ("s2" -> 2), ("s3" -> 3), ("s4" -> 4)), 2);
    //        var rdd22Result = rdd22.aggregateByKey(0)((acc, record) =>
    //          {
    //            print("sfdgfhg")
    //            acc+record
    //          }, (acc1, acc2) => {
    //                 acc1+acc2
    //        })
    //        rdd22.foreach(println)

    //    val myRDD = sparkSession.sparkContext.parallelize(List("Yellow", "Red", "Blue", "Cyan", "Black"), 3)
    //    val myAnotherRDD = myRDD.mapPartitionsWithIndex {
    //      // 'index' represents the Partition No.
    //      // 'iterator' to iterate through all elements in the partition
    //      (index, iterator) =>
    //        {
    //          println("Partition No. -> " + index)
    //          val myList = iterator.toList
    //          // In a normal user case, we will do the
    //          // the initialization(for example:- initializing database/configurations)
    //          // before iterating through each element
    //          myList.map(x => x + " -> " + index).iterator
    //        }
    //    }
    //
    //    myAnotherRDD.foreach(println)

    //     var rdd2 = sparkSession.sparkContext.parallelize(List(("s1" -> 1), ("s2" -> 2), ("s2" -> 3), ("s4" -> 4)));
    //     rdd2.keys.foreach(println)
    //     var rdd3 = rdd2.flatMap(kv => kv._1+kv._2);
    //     rdd3.foreach(print)
    //     println
    //     var rdd4 = rdd2.flatMapValues((v)=>v+"str1")
    //     rdd4.foreach(print)
    //     var rdd5 = rdd2.map(kv =>kv._1+kv._2).collect()
    //     rdd5.foreach(println)
    //
    //     var rdd6 = rdd2.reduce((v1,v2)=>{
    //       new Tuple2(v1._1+v2._1,v2._2+v1._2)
    //     })
    //     println(rdd6)
    //
    //     var rdd7 = rdd2.reduceByKey((v1,v2)=>v1+v2)
    //     rdd7.foreach(println)
    //
    //     var rdd8 = rdd2.fold(("s0",0))((v1,v2)=>{
    //       new Tuple2(v1._1+v2._1,v2._2+v1._2)
    //     })
    //     println(rdd8)
    //
    //     var rdd9 = rdd2.foldByKey(0)((v1,v2)=>v1+v2)
    //     rdd9.foreach(println)
  }

}

