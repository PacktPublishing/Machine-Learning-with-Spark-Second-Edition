/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sparksamples.kmeans

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans

import org.apache.spark.sql.SparkSession

/**
 */
object MovieLensKMeansMetrics {
  case class RatingX(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  val DATA_PATH= "../../../data/ml-100k"
  val PATH_MOVIES = DATA_PATH + "/u.item"
  val dataSetUsers = null

  def main(args: Array[String]): Unit = {

    val spConfig = (new SparkConf).setMaster("local[1]").setAppName("SparkApp").
      set("spark.driver.allowMultipleContexts", "true")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .config(spConfig)
      .getOrCreate()

    val datasetUsers = spark.read.format("libsvm").load(
      "./data/movie_lens_libsvm/movie_lens_users_libsvm/part-00000")
    datasetUsers.show(3)

    val k = 5
    val itr = Array(1,10,20,50,75,100)
    val result = new Array[String](itr.length)
    for(i <- 0 until itr.length){
      val w = calculateWSSSE(spark,datasetUsers,itr(i),5,1L)
      result(i) = itr(i) + "," + w
    }
    println("----------Users----------")
    for(j <- 0 until itr.length) {
      println(result(j))
    }
    println("-------------------------")

    val datasetItems = spark.read.format("libsvm").load(
      "./data/movie_lens_libsvm/movie_lens_items_libsvm/part-00000")
    val resultItems = new Array[String](itr.length)
    for(i <- 0 until itr.length){
      val w = calculateWSSSE(spark,datasetItems,itr(i),5,1L)
      resultItems(i) = itr(i) + "," + w
    }

    println("----------Items----------")
    for(j <- 0 until itr.length) {
      println(resultItems(j))
    }
    println("-------------------------")


    spark.stop()
  }

  import org.apache.spark.sql.DataFrame

  def calculateWSSSE(spark : SparkSession, dataset : DataFrame, iterations : Int, k : Int,
                     seed : Long) : Double = {
    val x = dataset.columns

    val kmeans = new KMeans().setK(k).setSeed(seed).setMaxIter(iterations)

    val model = kmeans.fit(dataset)
    val WSSSEUsers = model.computeCost(dataset)
    return WSSSEUsers

  }
}
