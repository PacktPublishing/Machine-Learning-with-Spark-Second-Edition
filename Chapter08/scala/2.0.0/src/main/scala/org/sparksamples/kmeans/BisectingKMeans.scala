/*
 *
 */

package org.sparksamples.kmeans

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.BisectingKMeans

import org.apache.spark.sql.SparkSession
import org.sparksamples.Util

/**
  *
  */
object BisectingKMeans {
  val PATH = "/home/ubuntu/work/spark-2.0.0-bin-hadoop2.7/"
  val BASE = "./data/movie_lens_libsvm_2f"

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
    val bKMeansUsers = new BisectingKMeans()
    bKMeansUsers.setMaxIter(10)
    bKMeansUsers.setMinDivisibleClusterSize(4)

    val modelUsers = bKMeansUsers.fit(datasetUsers)

    val movieDF = Util.getMovieDataDF(spark)
    val predictedUserClusters = modelUsers.transform(datasetUsers)
    predictedUserClusters.show(5)

    val joinedMovieDFAndPredictedCluster =
      movieDF.join(predictedUserClusters,predictedUserClusters("label") === movieDF("id"))
    print(joinedMovieDFAndPredictedCluster.first())
    joinedMovieDFAndPredictedCluster.show(5)

    for(i <- 0 until 5) {
      val prediction0 = joinedMovieDFAndPredictedCluster.filter("prediction == " + i)
      println("Cluster : " + i)
      println("--------------------------")
      prediction0.select("name").show(10)
    }

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSEUsers = modelUsers.computeCost(datasetUsers)
    println(s"Users :  Within Set Sum of Squared Errors = $WSSSEUsers")

    println("Users : Cluster Centers: ")
    modelUsers.clusterCenters.foreach(println)

    val datasetItems = spark.read.format("libsvm").load(
      BASE + "/movie_lens_2f_items_libsvm/part-00000")
    datasetItems.show(3)

    val kmeansItems = new BisectingKMeans().setK(5).setSeed(1L)
    val modelItems = kmeansItems.fit(datasetItems)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSEItems = modelItems.computeCost(datasetItems)
    println(s"Items :  Within Set Sum of Squared Errors = $WSSSEItems")

    // Shows the result.
    println("Items - Cluster Centers: ")
    modelUsers.clusterCenters.foreach(println)
    spark.stop()
  }
}
