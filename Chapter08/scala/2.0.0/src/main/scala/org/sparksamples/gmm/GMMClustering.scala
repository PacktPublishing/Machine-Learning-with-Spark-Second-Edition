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

package org.sparksamples.gmm

// scalastyle:off println

// $example on$
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{GaussianMixture, KMeans}
// $example off$
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
 */
object GMMClustering {


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

    val gmmUsers = new GaussianMixture().setK(5).setSeed(1L)
    val modelUsers = gmmUsers.fit(datasetUsers)

    for (i <- 0 until modelUsers.gaussians.length) {
      println("Users : weight=%f\ncov=%s\nmean=\n%s\n" format
        (modelUsers.weights(i), modelUsers.gaussians(i).cov, modelUsers.gaussians(i).mean))
    }

    val dataSetItems = spark.read.format("libsvm").load(
      "./data/movie_lens_libsvm/movie_lens_items_libsvm/part-00000")

    val gmmItems = new GaussianMixture().setK(5).setSeed(1L)
    val modelItems = gmmItems.fit(dataSetItems)

    for (i <- 0 until modelItems.gaussians.length) {
      println("Items : weight=%f\ncov=%s\nmean=\n%s\n" format
        (modelUsers.weights(i), modelUsers.gaussians(i).cov, modelUsers.gaussians(i).mean))
    }

    spark.stop()
  }

  def loadInLibSVMFormat(line: String, noOfFeatures : Int) : LabeledPoint = {
    val items = line.split(' ')
    val label = items.head.toDouble
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
    val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip

    // check if indices are one-based and in ascending order
    var previous = -1
    var i = 0
    val indicesLength = indices.length
    while (i < indicesLength) {
      val current = indices(i)
      require(current > previous, "indices should be one-based and in ascending order" )
      previous = current
      i += 1
    }

    (label, indices.toArray, values.toArray)

    import org.apache.spark.mllib.linalg.Vectors
    val d = noOfFeatures
    LabeledPoint(label, Vectors.sparse(d, indices, values))
  }
}
