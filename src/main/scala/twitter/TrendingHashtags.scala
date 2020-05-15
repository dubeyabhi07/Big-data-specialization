package twitter

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TrendingHashtags {

  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load("application.properties")
    System.setProperty("twitter4j.oauth.consumerKey", props.getString("twitter4j.oauth.consumerKey"))
    System.setProperty("twitter4j.oauth.consumerSecret", props.getString("twitter4j.oauth.consumerSecret"))
    System.setProperty("twitter4j.oauth.accessToken", props.getString("twitter4j.oauth.accessToken"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", props.getString("twitter4j.oauth.accessTokenSecret"))

    val sparkSess = SparkSession.builder()
      .appName("twitter-trending-hashtags")
      .master("local[2]")
      .getOrCreate()
    val sc = sparkSess.sparkContext
    val streamingContext = new StreamingContext(sc, Seconds(5))
    val sparkContext = sparkSess.sparkContext
    sparkContext.setLogLevel("ERROR")

    val stream = TwitterUtils.createStream(streamingContext, None)

    val hashTags = stream.flatMap(status => status.getText.split(" ")
      .filter(words => words.startsWith("#")))

    // Get the top hashtags over the previous 60 sec window
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Get the top hashtags over the previous 10 sec window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // print tweets in the currect DStream
    // println(stream.print())

    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    streamingContext.start();
    streamingContext.awaitTermination();

  }
}
