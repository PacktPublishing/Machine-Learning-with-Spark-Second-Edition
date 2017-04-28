package org.sparksamples.featureext

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
/**
  * @author Rajdeep Dua
  *         March 4 2016
  */
object TfIdfSample{
  def main(args: Array[String]) {
    //TODO replace with path specific to your machine
    val file = "/home/ubuntu/work/spark-1.6.0-bin-hadoop2.6//README.md"
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val sc = new SparkContext(spConfig)
    val documents: RDD[Seq[String]] = sc.textFile(file).map(_.split(" ").toSeq)
    print("Documents Size:" + documents.count)
    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(documents)
    for(tf_ <- tf) {
      println(s"$tf_")
    }
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    println("tfidf size : " + tfidf.count)
    for(tfidf_ <- tfidf) {
      println(s"$tfidf_")
    }
  }
}