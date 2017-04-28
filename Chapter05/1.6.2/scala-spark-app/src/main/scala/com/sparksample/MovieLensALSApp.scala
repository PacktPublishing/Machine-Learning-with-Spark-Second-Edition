package com.sparksample

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.jblas.DoubleMatrix

/**
  * ALS applied to MovieLens Data
  * @ author Rajdeep Dua
  * March 2016
  */
object MovieLensALSApp {

  //val PATH = "../../data"
  def main(args: Array[String]) {
    val sc = Util.sc
    val rawData = Util.getUserData()
    rawData.first()

    /* Extract the user id, movie id and rating only from the dataset */
    val rawRatings = rawData.map(_.split("\t").take(3))
    println(rawRatings.first())

    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    val ratingsFirst = ratings.first()
    println(ratingsFirst)

    /* Train the ALS model with rank=50, iterations=10, lambda=0.01 */
    val model = ALS.train(ratings, 50, 10, 0.01)
    val model2 = ALS.train(ratings, 50, 10, 0.009)

    /* Inspect the user factors */
    println( model.userFeatures)
    /* Count user factors and force computation */
    println("userFeatures.count:" + model.userFeatures.count)
    println("productFeatures.count" + model.productFeatures.count)

    /* Make a prediction for a single user and movie pair */
    val predictedRating = model.predict(789, 123)
    println(predictedRating)
    val userId = 789
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
    println(topKRecs.mkString("\n"))

    val movies = Util.getMovieData()
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    titles(123)
    // res68: String = Frighteners, The (1996)
    val moviesForUser = ratings.keyBy(_.user).lookup(789)

    println(moviesForUser.size)
    //moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)


    val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))
    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head
    // itemFactor: Array[Double] = Array(0.15179359424040248, -0.2775955241896113, 0.9886005994661484, ...
    val itemVector = new DoubleMatrix(itemFactor)
    // itemVector: org.jblas.DoubleMatrix = [0.151794; -0.277596; 0.988601; -0.464013; 0.188061; 0.090506; ...
    Util.cosineSimilarity(itemVector, itemVector)
    // res113: Double = 1.0000000000000002
    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = Util.cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    // sortedSims: Array[(Int, Double)] = Array((567,1.0), (672,0.483244928887981), (1065,0.43267674923450905), ...
    println(sortedSims.mkString("\n"))

    println(titles(itemId))
    // Wes Craven's New Nightmare (1994)
    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    sortedSims2.slice(1, 11).map{ case (id, sim) => (titles(id), sim) }.mkString("\n")

    val actualRating = moviesForUser.take(1)(0)
    // actualRating: Seq[org.apache.spark.mllib.recommendation.Rating] = WrappedArray(Rating(789,1012,4.0))
    val predictedRating2 = model.predict(789, actualRating.product)
    // ...
    // 14/04/13 13:01:15 INFO SparkContext: Job finished: lookup at MatrixFactorizationModel.scala:46, took 0.025404 s
    // predictedRating: Double = 4.001005374200248
    val squaredError = math.pow(predictedRating2 - actualRating.rating, 2.0)

    println("Squared error for 789:" + squaredError)

    val usersProducts = ratings.map{ case Rating(user, product, rating)  => (user, product)}
    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rating) => ((user, product), rating)
    }
    val ratingsAndPredictions = ratings.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions)

    val MSE = ratingsAndPredictions.map{
      case ((user, product), (actual, predicted)) =>  math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)

    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)

    val predictions2 = model2.predict(usersProducts).map{
      case Rating(user, product, rating) => ((user, product), rating)
    }
    val ratingsAndPredictions2 = ratings.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions)

    val MSE2 = ratingsAndPredictions2.map{
      case ((user, product), (actual, predicted)) =>  math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions2.count
    println("Mean Squared Error 2= " + MSE2)

    val RMSE2 = math.sqrt(MSE2)
    println("Root Mean Squared Error = " + RMSE2)

    val actualMovies = moviesForUser.map(_.product)
    // actualMovies: Seq[Int] = ArrayBuffer(1012, 127, 475, 93, 1161, 286, 293, 9, 50, 294, 181, 1, 1008, 508, 284, 1017, 137, 111, 742, 248, 249, 1007, 591, 150, 276, 151, 129, 100, 741, 288, 762, 628, 124)
    val predictedMovies = topKRecs.map(_.product)
    // predictedMovies: Array[Int] = Array(27, 497, 633, 827, 602, 849, 401, 584, 1035, 1014)
    val apk10 = Util.avgPrecisionK(actualMovies, predictedMovies, 10)
    val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)
    // (1682,50)

    Util.sc.stop()
  }

}
