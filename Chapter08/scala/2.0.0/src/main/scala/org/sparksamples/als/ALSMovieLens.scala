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

// scalastyle:off println
package org.sparksamples.als

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import java.util.Date
import java.util.SimpleTimeZone
import java.text.SimpleDateFormat


// $example on$
// $example off$

/**
 * An example demonstrating ALS.
 */
object ALSMovieLens {
  val PATH = "/home/ubuntu/work/spark-2.0.0-bin-hadoop2.7/";
  val DATA_PATH= "../../../data/ml-100k"
  val time = System.currentTimeMillis()
  val formatter = new SimpleDateFormat("dd_MM_yyyy_hh_mm_ss")

  import java.util.Calendar
  val calendar = Calendar.getInstance()
  calendar.setTimeInMillis(time)
  val date_time = formatter.format(calendar.getTime())

  val output = "./OUTPUT"

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]) {
    getUserVectors()
  }

  def getUserVectors(): RDD[String] = {
    import org.apache.spark.sql.SparkSession
    val spConfig = (new SparkConf).setMaster("local[1]").setAppName("SparkApp").
      set("spark.driver.allowMultipleContexts", "true")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .config(spConfig)
      .getOrCreate()

    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val ratings = spark.sparkContext
      .textFile(DATA_PATH + "/u.data")
      .map(_.split("\t"))
      .map(lineSplit => Rating(lineSplit(0).toInt, lineSplit(1).toInt, lineSplit(2).toFloat, lineSplit(3).toLong))
      .toDF()
    ratings.show(5)

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    //als.
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    val itemFactors = model.itemFactors
    itemFactors.show()


    val userFactors = model.userFactors
    userFactors.show()

    val itemVectors = itemFactors.rdd.map(x => x(1))
    val itemFactorfirst = itemFactors.first()

    def getDetails(itemVector : scala.collection.mutable.WrappedArray[Float]) : String = {
      var itemVectorString = ""
      for(i <- 0 until itemVector.length) {
        itemVectorString += (i+1) + ":" + itemVector(i)
        if (i < (itemVector.length -1)){
          itemVectorString = itemVectorString + " "
        }
      }
      return itemVectorString
    }
    val itemFactorsOrdererd = itemFactors.orderBy("id")
    val itemFactorLibSVMFormat = itemFactorsOrdererd.rdd.map(x => x(0) + " " +
      getDetails(x(1).asInstanceOf[scala.collection.mutable.WrappedArray[Float]]))
    println("itemFactorLibSVMFormat.count() : " + itemFactorLibSVMFormat.count())
    print("itemFactorLibSVMFormat.first() : " + itemFactorLibSVMFormat.first())

    itemFactorLibSVMFormat.coalesce(1).saveAsTextFile(output + "/" + date_time + "/movie_lens_items_libsvm")

    var itemFactorsXY = itemFactorsOrdererd.rdd.map(
      x => getXY(x(1).asInstanceOf[scala.collection.mutable.WrappedArray[Float]]))
    itemFactorsXY.first()
    itemFactorsXY.coalesce(1).saveAsTextFile(output + "/" + date_time + "/movie_lens_items_xy")


    val userFactorsOrdererd = userFactors.orderBy("id")
    val userFactorLibSVMFormat = userFactorsOrdererd.rdd.map(x => x(0) + " " +
      getDetails(x(1).asInstanceOf[scala.collection.mutable.WrappedArray[Float]]))
    println("userFactorLibSVMFormat.count() : " + userFactorLibSVMFormat.count())
    print("userFactorLibSVMFormat.first() : " + userFactorLibSVMFormat.first())

    userFactorLibSVMFormat.coalesce(1).saveAsTextFile(output + "/" + date_time + "/movie_lens_users_libsvm")

    var userFactorsXY = userFactorsOrdererd.rdd.map(
      x => getXY(x(1).asInstanceOf[scala.collection.mutable.WrappedArray[Float]]))
    userFactorsXY.first()
    userFactorsXY.coalesce(1).saveAsTextFile(output + "/" + date_time + "/movie_lens_user_xy")


    spark.stop()
    return itemFactorLibSVMFormat
  }

  def getXY(itemVector : scala.collection.mutable.WrappedArray[Float]) : String = {
    var itemVectorString = ""
    var half = 0
    var x = 0.0
    var y = 0.0

    if (itemVector.length % 2 == 0) {
      half = itemVector.length / 2
    }else {
      half = (itemVector.length + 1) /2
    }
    for(i <- 0 until half) {
      x +=  itemVector(i)
    }
    for(j <- half until itemVector.length) {
      y +=  itemVector(j)
    }
    return x + ", " + y
  }
}

