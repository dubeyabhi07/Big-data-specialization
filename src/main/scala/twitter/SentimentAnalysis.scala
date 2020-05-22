package twitter

import sessioninit.Session
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.HashtagEntity
import org.apache.spark.SparkContext
import edu.stanford.nlp.util.CoreMap
import java.util.Properties

import com.typesafe.config.ConfigFactory
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SentimentAnalysis {

  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load("application.properties")
    System.setProperty("twitter4j.oauth.consumerKey", props.getString("twitter4j.oauth.consumerKey"))
    System.setProperty("twitter4j.oauth.consumerSecret", props.getString("twitter4j.oauth.consumerSecret"))
    System.setProperty("twitter4j.oauth.accessToken", props.getString("twitter4j.oauth.accessToken"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", props.getString("twitter4j.oauth.accessTokenSecret"))

    val sparkSess = Session.startSparkSession
    val sc = sparkSess.sparkContext
    val streamingContext = new StreamingContext(sc, Seconds(5))

    val tweets = TwitterUtils.createStream(streamingContext, None, Array("India"))
    tweets.map(status => status.getText).map(tweet => (tweet, sentiment(tweet)))
      .foreachRDD(rdd => rdd.collect().foreach(tuple => println(" Sentiment => " + tuple._2 + " :-: TWEET => " + tuple._1)))

    streamingContext.start();
    streamingContext.awaitTermination();
  }

  def sentiment(tweets: String): String = {
    var mainSentiment = 0
    val sentimentText = Array("Very Negative", "Negative", "Neutral", "Positive", "Very Positive")
    val props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
    new StanfordCoreNLP(props)
      .process(tweets)
      .get(classOf[CoreAnnotations.SentencesAnnotation])
      .asScala
      .foreach((sentence: CoreMap) => {
      val sentiment = RNNCoreAnnotations
        .getPredictedClass(sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree]));
      mainSentiment = sentiment;
    })
    sentimentText(mainSentiment)
  }
}