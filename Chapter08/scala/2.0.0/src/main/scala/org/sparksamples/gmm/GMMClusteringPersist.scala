package org.sparksamples.gmm

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{GaussianMixture}
import org.apache.spark.sql.SparkSession

/**
  *
  */
object GMMClusteringPersist {
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
      BASE + "/movie_lens_2f_users_libsvm/part-00000")
    datasetUsers.show(3)

    val gmmUsers = new GaussianMixture().setK(5).setSeed(1L)
    gmmUsers.setMaxIter(20)
    val modelUsers = gmmUsers.fit(datasetUsers)

    val predictedDataSetUsers = modelUsers.transform(datasetUsers)
    val predictionsUsers = predictedDataSetUsers.select("prediction").rdd.map(x=> x(0))
    predictionsUsers.saveAsTextFile(BASE + "/prediction/" + date_time + "/gmm_2f_users")


    val dataSetItems = spark.read.format("libsvm").load(BASE +
      "/movie_lens_2f_items_libsvm/part-00000")


    val gmmItems = new GaussianMixture().setK(5).setSeed(1L)
    val modelItems = gmmItems.fit(dataSetItems)

    val predictedDataSetItems = modelItems.transform(dataSetItems)
    val predictionsItems = predictedDataSetItems.select("prediction").rdd.map(x=> x(0))
    predictionsItems.saveAsTextFile(BASE + "/prediction/" + date_time + "/gmm_2f_items")
    spark.stop()
  }
}
