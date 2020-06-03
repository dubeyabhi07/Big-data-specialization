package event

import com.typesafe.config.ConfigFactory
import sessioninit.Session
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{col, explode, lit, map, map_concat, map_from_entries, map_keys, map_values}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}


object SimpleToComplex {

  val sparkSession = Session.startSparkSession;

  import sparkSession.sqlContext.implicits._

  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load("application.properties");

    /**
     * processing schedule.csv (was saved in ComplexToSimple.scala)
     */

    var schDf = sparkSession.read.option("inferSchema", "true").
      option("header", "true").csv(props.getString("scheduleCsv"))


    schDf = schDf.withColumn("detail", struct('cost, 'date))
      .drop('cost).drop('date)
      .groupBy('event_id, 'city)
      .agg(collect_list("detail") as "details")
      .select('event_id, map('city, 'details).as("schedule_per_city"))
      .groupBy('event_id).agg(collect_list("schedule_per_city") as "schedule")

    val innerMapSchema = new MapType(StringType, StringType, true)
    val arrSchema = new ArrayType(innerMapSchema, true)
    val mapSchema = new MapType(StringType, arrSchema, true)
    val scheduleSchema = new MapType(StringType, mapSchema, true)

    schDf = schDf.select('event_id, to_json('schedule) as 'scheduleStr)
      .withColumn("processedScheduleStr", listToMapUdf('scheduleStr))
      .withColumn("schedule", from_json(col("processedScheduleStr"), scheduleSchema))
      .drop('processedScheduleStr).drop('scheduleStr)

    println("consolidated schedule Schema .........................................")
    schDf.show(10)


    /**
     * processing reserved.json (was saved in ComplexToSimple.scala)
     */
    var resDf = sparkSession.read.option("multiline", "true")
      .json(props.getString("reservedJson"))

    val confirmedDf = processConfirmPart(resDf)
    val waitlistDf = processWaitlistPart(resDf)

    resDf = confirmedDf.join(waitlistDf, waitlistDf("event_id") === confirmedDf("event_id"))
      .drop(confirmedDf("event_id"))
      .select('event_id, struct('confirmed, 'waitlist).as("reserved"))

    println("consolidated reserved Schema .........................................")
    resDf.show(10)

    var finalDf = resDf.join(schDf, schDf("event_id") === resDf("event_id"))
      .select(schDf("event_id"), 'reserved, 'schedule)
      .select(struct('event_id, $"schedule.schedule", 'reserved).as("event_data"))

    println("Final Output .........................................")
    finalDf.show(10)

  }

  def listToMapUdf = udf((col: String) => {
    var str = col.replace("]},{", "],");
    str = str.substring(1, str.length() - 1)
    "{'schedule': " + str + "}"
  })

  def processConfirmPart(resDf: DataFrame): DataFrame = {
    var df = resDf.select('event_id, 'details, 'confirmed_city, 'total_confirmed_slots)
      .where(col("confirmed_city") !== "")
      .drop('total_confirmed_slots)
      .select('event_id, 'confirmed_city.as("city"), explode('details).as("detail"))
      .select('event_id, struct('city, $"detail.address", $"detail.slots").as("confirmed"))
      .groupBy('event_id).agg(collect_list('confirmed).as("confirmed"))
    df
  }

  def processWaitlistPart(resDf: DataFrame):DataFrame = {
    var df = resDf.select('event_id,'waitlist_city.as("city"),'total_waitlist_slots.as("slots"))
      .where(col("waitlist_city") !== "")
      .select('event_id,struct('city,'slots).as("detail"))
      .groupBy('event_id).agg(collect_list('detail).as("waitlist"))
    df
  }
}
