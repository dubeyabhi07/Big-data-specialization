package event

import climate.Utility.props
import com.typesafe.config.ConfigFactory
import sessioninit.Session
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{col, explode, lit, map, map_concat, map_from_entries, map_keys, map_values}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

//creating a case class as StructType can't be passed in UDF
case class scheduleDetail(
                           cost: String,
                           date: String
                         )

object JsonToCsv {
  val sparkSession = Session.startSparkSession;

  import sparkSession.sqlContext.implicits._

  def main(args: Array[String]): Unit = {


    val props = ConfigFactory.load("application.properties");

    var eventJsonDf = sparkSession.read.option("multiline", "true").
      json(props.getString("eventDataJson"))
    eventJsonDf.show()

    //removing the cover, retrieving data and converting the outer fields into columns
    val df = eventJsonDf.select(explode($"event_data").as("temp_field"))
      .select($"temp_field.*")
    df.show()


    var reservedDf = df.select('event_id, 'reserved)
    reservedDf = getFlattenedReservedDf(reservedDf: DataFrame)
    var scheduleDf = df.select('event_id, 'schedule)
    scheduleDf = getFlattenedScheduleDf(scheduleDf)

    //TODO : save these dataframes
  }

  def getFlattenedReservedDf(reservedDf: DataFrame): DataFrame = {

    var df = reservedDf.select('event_id,$"reserved.*")
    var confirmedDf = df.withColumn("confirmed_entries", explode($"confirmed")).
      drop('confirmed).drop('waitlist)
    confirmedDf = getFlattenedConfirmedReservations(confirmedDf)
    var waitlistDf = df.withColumn("waitlisted_entries", explode($"waitlist")).
      drop('confirmed).drop('waitlist)
    waitlistDf = getFlattenedWaitlistedReservations(waitlistDf)

    df = waitlistDf.join(confirmedDf,
      confirmedDf("event_id") === waitlistDf("event_id") &&
        waitlistDf("waitlist_city") === confirmedDf("confirmed_city"), "outer")
      .select(confirmedDf("event_id"), 'confirmed_city, 'details,
        'total_confirmed_slots, 'waitlist_city, 'total_waitlist_slots)
      .na.drop("all", Seq("event_id"))

    println("reserved structured flattened .........................................")
    df.show()
    df
  }

  def getFlattenedScheduleDf(scheduleDf: DataFrame): DataFrame = {
    val cols = scheduleDf.select($"schedule.*").columns
    var df = scheduleDf.select('event_id, $"schedule.*")

    //converting this to array as zip need to iterables
    df = df.withColumn("all", array(cols.head, cols.tail: _*))

    //UDF function to zip
    def collectUdf = udf((cols: collection.mutable.WrappedArray[String],
                          values: collection.mutable.WrappedArray[
                            collection.mutable.WrappedArray[scheduleDetail]]
                         ) => cols.zip(values))

    df = df.withColumn("city_events_map", collectUdf(lit(cols), df("all")))
      .withColumn("city_event_exploded", explode($"city_events_map"))

    df = df.select('event_id, $"city_event_exploded._1".as("city"), $"city_event_exploded._2".as("schedules"))
      .withColumn("schedule", explode($"schedules"))
      .select('event_id, 'city, $"schedule.*")

    println("schedule structure flattened .........................................")
    df.show()
    df
  }

  def getFlattenedConfirmedReservations(confirmedDf: DataFrame): DataFrame = {
    var df = confirmedDf.withColumn("confirmed_entry_map",
      map($"confirmed_entries.city",
        struct($"confirmed_entries.address", $"confirmed_entries.slots")))
      .withColumn("slots", $"confirmed_entries.slots")
      .drop('confirmed_entries)

    df = df.select('event_id, explode($"confirmed_entry_map"), 'slots)
      .select('event_id, $"key".as("confirmed_city"), 'value.as("details"), 'slots)

    df = df.groupBy($"event_id", $"confirmed_city")
      .agg(collect_list("details") as "details",
        sum(col("slots")).as("total_confirmed_slots"))

    println("confiremed reservation structure flattened .........................................")
    df.show()
    df
  }

  def getFlattenedWaitlistedReservations(waitlistDf: DataFrame): DataFrame = {
    var df = waitlistDf.select('event_id, $"waitlisted_entries.*")
    df = df.groupBy($"event_id", $"city".as("waitlist_city"))
      .agg(sum(col("slots")).as("total_waitlist_slots"))

    println("waitlist reservation structure flattened .........................................")
    df.show()
    df
  }
}
