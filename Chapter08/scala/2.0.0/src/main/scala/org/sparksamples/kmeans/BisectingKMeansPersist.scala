/*
 *
 */

package org.sparksamples.kmeans

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.sql.SparkSession

/**
  *
  */
object BisectingKMeansPersist {
  val PATH = "/home/ubuntu/work/spark-2.0.0-bin-hadoop2.7/"
  val BASE = "./data/movie_lens_libsvm_2f"

  val time = System.currentTimeMillis()
  val formatter = new SimpleDateFormat("dd_MM_yyyy_hh_mm_ss")

  import java.util.Calendar
  val calendar = Calendar.getInstance()
  calendar.setTimeInMillis(time)
  val date_time = formatter.format(calendar.getTime())

  def main(args: Array[String]): Unit = {

    val spConfig = (new SparkConf).setMaster("local[1]").setAppName("SparkApp").
      set("spark.driver.allowMultipleContexts", "true")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .config(spConfig)
      .getOrCreate()

    val datasetUsers = spark.read.format("libsvm").load(
      BASE + "/movie_lens_2f_users_xy/part-00000")
    datasetUsers.show(3)
    val bKMeansUsers = new BisectingKMeans()
    bKMeansUsers.setMaxIter(10)
    bKMeansUsers.setMinDivisibleClusterSize(5)

    val modelUsers = bKMeansUsers.fit(datasetUsers)
    val predictedUserClusters = modelUsers.transform(datasetUsers)

    modelUsers.clusterCenters.foreach(println)
    val predictedDataSetUsers = modelUsers.transform(datasetUsers)
    val predictionsUsers = predictedDataSetUsers.select("prediction").rdd.map(x=> x(0))
    predictionsUsers.saveAsTextFile(BASE + "/prediction/" + date_time + "/bkmeans_2f_users")


    val datasetItems = spark.read.format("libsvm").load(BASE +
      "/movie_lens_2f_items_xy/part-00000")
    datasetItems.show(3)

    val kmeansItems = new BisectingKMeans().setK(5).setSeed(1L)
    val modelItems = kmeansItems.fit(datasetItems)


    val predictedDataSetItems = modelItems.transform(datasetItems)
    val predictionsItems = predictedDataSetItems.select("prediction").rdd.map(x=> x(0))
    predictionsItems.saveAsTextFile(BASE + "/prediction/" + date_time + "/bkmeans_2f_items")
    spark.stop()
  }
}
