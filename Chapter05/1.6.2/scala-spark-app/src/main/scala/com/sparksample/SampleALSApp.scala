package com.sparksample

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.jblas.DoubleMatrix

import scala.collection.JavaConverters._
import scala.math._
import scala.util.Random

/**
  * Created by ubuntu on 3/10/16.
  */
object SampleALSApp {
  def main(args: Array[String]) {
    runALS(50, 100, 1, 15, 0.7, 0.3)
  }

  def generateRatingsAsJavaList(
                                 users: Int,
                                 products: Int,
                                 features: Int,
                                 samplingRate: Double,
                                 implicitPrefs: Boolean,
                                 negativeWeights: Boolean): (java.util.List[Rating], DoubleMatrix, DoubleMatrix) = {
    val (sampledRatings, trueRatings, truePrefs) =
      generateRatings(users, products, features, samplingRate, implicitPrefs)
    (sampledRatings.asJava, trueRatings, truePrefs)
  }

  def generateRatings(
                       users: Int,
                       products: Int,
                       features: Int,
                       samplingRate: Double,
                       implicitPrefs: Boolean = false,
                       negativeWeights: Boolean = false,
                       negativeFactors: Boolean = true): (Seq[Rating], DoubleMatrix, DoubleMatrix) = {
    val rand = new Random(42)

    // Create a random matrix with uniform values from -1 to 1

    val userMatrix = randomMatrix(users, features, negativeFactors)
    val productMatrix = randomMatrix(features, products, negativeFactors)
    val (trueRatings, truePrefs) = implicitPrefs match {
      case true =>
        // Generate raw values from [0,9], or if negativeWeights, from [-2,7]
        val raw = new DoubleMatrix(users, products,
          Array.fill(users * products)(
            (if (negativeWeights) -2 else 0) + rand.nextInt(10).toDouble): _*)
        val prefs =
          new DoubleMatrix(users, products, raw.data.map(v => if (v > 0) 1.0 else 0.0): _*)
        (raw, prefs)
      case false => (userMatrix.mmul(productMatrix), null)
    }

    val sampledRatings = {
      for (u <- 0 until users; p <- 0 until products if rand.nextDouble() < samplingRate)
        yield Rating(u, p, trueRatings.get(u, p))
    }

    (sampledRatings, trueRatings, truePrefs)
  }
  def randomMatrix(m: Int, n: Int, negativeFactors: Boolean) = {
    val rand = new Random(42)
    if (negativeFactors) {
      new DoubleMatrix(m, n, Array.fill(m * n)(rand.nextDouble() * 2 - 1): _*)
    } else {
      new DoubleMatrix(m, n, Array.fill(m * n)(rand.nextDouble()): _*)
    }
  }


  def runALS(
              users: Int,
              products: Int,
              features: Int,
              iterations: Int,
              samplingRate: Double,
              matchThreshold: Double,
              implicitPrefs: Boolean = true,
              bulkPredict: Boolean = false,
              negativeWeights: Boolean = false,
              numUserBlocks: Int = -1,
              numProductBlocks: Int = -1,
              negativeFactors: Boolean = true) {

    val (sampledRatings, trueRatings, truePrefs) = generateRatings(users, products,
      features, samplingRate, implicitPrefs, negativeWeights, negativeFactors)

    //val sc = new SparkContext("local[2]", "Chapter 5 App")
    val sc = Util.sc
    val modelx = new ALS()
    /*.setUserBlocks(numUserBlocks)
    .setProductBlocks(numProductBlocks)
    .setRank(features)
    .setIterations(iterations)
    .setAlpha(1.0)
    .setImplicitPrefs(implicitPrefs)
    .setLambda(0.01)
    .setSeed(0L)
    .setNonnegative(!negativeFactors)*/
    //.run(sc.parallelize(sampledRatings))
    val sampleRatingsRDD = sc.parallelize(sampledRatings)
    //val model = modelx.run(sampleRatingsRDD)
    val model = ALS.train(sampleRatingsRDD, 50, 10, 0.01)

    val predictedU = new DoubleMatrix(users, features)
    for ((u, vec) <- model.userFeatures.collect(); i <- 0 until features) {
      predictedU.put(u, i, vec(i))
    }
    val predictedP = new DoubleMatrix(products, features)
    for ((p, vec) <- model.productFeatures.collect(); i <- 0 until features) {
      predictedP.put(p, i, vec(i))
    }
    val predictedRatings = bulkPredict match {
      case false => predictedU.mmul(predictedP.transpose)
      case true =>
        val allRatings = new DoubleMatrix(users, products)
        val usersProducts = for (u <- 0 until users; p <- 0 until products) yield (u, p)
        val userProductsRDD = sc.parallelize(usersProducts)
        model.predict(userProductsRDD).collect().foreach { elem =>
          allRatings.put(elem.user, elem.product, elem.rating)
        }
        allRatings
    }

    if (!implicitPrefs) {
      for (u <- 0 until 10; p <- 0 until 5) {
        val prediction = predictedRatings.get(u, p)
        val correct = trueRatings.get(u, p)
        println("Prediction:" + prediction)
        println("Correct:" + correct)
        if (math.abs(prediction - correct) > matchThreshold) {
          println(("Model failed to predict (%d, %d): %f vs %f\ncorr: %s\npred: %s\nU: %s\n P: %s")
            .format(u, p, correct, prediction, trueRatings, predictedRatings, predictedU,
              predictedP))
        }
      }
    } else {
      // For implicit prefs we use the confidence-weighted RMSE to test (ref Mahout's tests)
      var sqErr = 0.0
      var denom = 0.0
      for (u <- 0 until users; p <- 0 until products) {
        val prediction = predictedRatings.get(u, p)
        val truePref = truePrefs.get(u, p)
        val confidence = 1 + 1.0 * abs(trueRatings.get(u, p))
        val err = confidence * (truePref - prediction) * (truePref - prediction)
        sqErr += err
        denom += confidence
      }
      val rmse = math.sqrt(sqErr / denom)
      println("rmse:" + rmse)
      println("matchThreshold:" +  matchThreshold)
      if (rmse > matchThreshold) {
        println("Model failed to predict RMSE: %f\ncorr: %s\npred: %s\nU: %s\n P: %s".format(
          rmse, truePrefs, predictedRatings, predictedU, predictedP))
      }
    }
  }
}
