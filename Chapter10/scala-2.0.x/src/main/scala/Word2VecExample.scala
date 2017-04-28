/**
  * Created by ubuntu on 1/12/17.
  */

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
// $example off$
import org.apache.spark.sql.SparkSession

object Word2VecExample {
  def main(args: Array[String]) {
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder
      .appName("Word2Vec example").config(spConfig)
      .getOrCreate()

    val documentDF1 = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply))
    documentDF1.show(1)

    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")


    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
