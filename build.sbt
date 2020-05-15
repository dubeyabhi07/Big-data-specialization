name := "big-data-spark"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-mllib" % "2.4.3",
  "com.typesafe" % "config" % "1.4.0",
  "org.scalafx" %% "scalafx" % "12.0.2-R18",
  "org.apache.spark" %% "spark-streaming" % "2.4.3" % "provided",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"
)

