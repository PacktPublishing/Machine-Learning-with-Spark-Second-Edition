/*
	This code is intended to be run in the Scala shell. 
	Launch the Scala Spark shell by running ./bin/spark-shell from the Spark directory.
	You can enter each line in the shell and see the result immediately.
	The expected output in the Spark console is presented as commented lines following the
	relevant code

	The Scala shell creates a SparkContex variable available to us as 'sc'

  Ensure you start the shell with sufficient memory: ./bin/spark-shell --driver-memory 4g
*/

/* Load the raw ratings data from a file. Replace 'PATH' with the path to the MovieLens data */
val rawData = sc.textFile("/PATH/ml-100k/u.data")
rawData.first()
// 14/03/30 13:21:25 INFO SparkContext: Job finished: first at <console>:17, took 0.002843 s
// res24: String = 196	242	3	881250949

/* Extract the user id, movie id and rating only from the dataset */
val rawRatings = rawData.map(_.split("\t").take(3))
rawRatings.first()
// 14/03/30 13:22:44 INFO SparkContext: Job finished: first at <console>:21, took 0.003703 s
// res25: Array[String] = Array(196, 242, 3)

/* Import Spark's ALS recommendation model and inspect the train method */
import org.apache.spark.mllib.recommendation.ALS
ALS.train
/*
	<console>:13: error: ambiguous reference to overloaded definition,
	both method train in object ALS of type (ratings: org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating], rank: Int, iterations: Int)org.apache.spark.mllib.recommendation.MatrixFactorizationModel
	and  method train in object ALS of type (ratings: org.apache.spark.rdd.RDD[org.apache.spark.mllib.recommendation.Rating], rank: Int, iterations: Int, lambda: Double)org.apache.spark.mllib.recommendation.MatrixFactorizationModel
	match expected type ?
    	          ALS.train
        	          ^ 
*/

/* Import the Rating class and inspect it */
import org.apache.spark.mllib.recommendation.Rating
Rating()
/*
	<console>:13: error: not enough arguments for method apply: (user: Int, product: Int, rating: Double)org.apache.spark.mllib.recommendation.Rating in object Rating.
	Unspecified value parameters user, product, rating.
    	          Rating()
        	            ^
*/


/* Construct the RDD of Rating objects */
val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
ratings.first()
// 14/03/30 13:26:43 INFO SparkContext: Job finished: first at <console>:24, took 0.002808 s
// res28: org.apache.spark.mllib.recommendation.Rating = Rating(196,242,3.0)

/* Train the ALS model with rank=50, iterations=10, lambda=0.01 */
val model = ALS.train(ratings, 50, 10, 0.01)
// ...
// 14/03/30 13:28:44 INFO MemoryStore: ensureFreeSpace(128) called with curMem=7544924, maxMem=311387750
// 14/03/30 13:28:44 INFO MemoryStore: Block broadcast_120 stored as values to memory (estimated size 128.0 B, free 289.8 MB)
// model: org.apache.spark.mllib.recommendation.MatrixFactorizationModel = org.apache.spark.mllib.recommendation.MatrixFactorizationModel@7c7fbd3b

/* Inspect the user factors */
model.userFeatures
// res29: org.apache.spark.rdd.RDD[(Int, Array[Double])] = FlatMappedRDD[1099] at flatMap at ALS.scala:231

/* Count user factors and force computation */
model.userFeatures.count
// ...
// 14/03/30 13:30:08 INFO SparkContext: Job finished: count at <console>:26, took 5.009689 s
// res30: Long = 943

model.productFeatures.count
// ...
// 14/03/30 13:30:59 INFO SparkContext: Job finished: count at <console>:26, took 0.247783 s
// res31: Long = 1682

/* Make a prediction for a single user and movie pair */ 
val predictedRating = model.predict(789, 123)

/* Make predictions for a single user across all movies */
val userId = 789
val K = 10
val topKRecs = model.recommendProducts(userId, K)
println(topKRecs.mkString("\n"))
/* 
Rating(789,715,5.931851273771102)
Rating(789,12,5.582301095666215)
Rating(789,959,5.516272981542168)
Rating(789,42,5.458065302395629)
Rating(789,584,5.449949837103569)
Rating(789,750,5.348768847643657)
Rating(789,663,5.30832117499004)
Rating(789,134,5.278933936827717)
Rating(789,156,5.250959077906759)
Rating(789,432,5.169863417126231)
*/

/* Load movie titles to inspect the recommendations */
val movies = sc.textFile("/PATH/ml-100k/u.item")
val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
titles(123)
// res68: String = Frighteners, The (1996)
val moviesForUser = ratings.keyBy(_.user).lookup(789)
// moviesForUser: Seq[org.apache.spark.mllib.recommendation.Rating] = WrappedArray(Rating(789,1012,4.0), Rating(789,127,5.0), Rating(789,475,5.0), Rating(789,93,4.0), ...
// ...
println(moviesForUser.size)
// 33
moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
/*
(Godfather, The (1972),5.0)
(Trainspotting (1996),5.0)
(Dead Man Walking (1995),5.0)
(Star Wars (1977),5.0)
(Swingers (1996),5.0)
(Leaving Las Vegas (1995),5.0)
(Bound (1996),5.0)
(Fargo (1996),5.0)
(Last Supper, The (1995),5.0)
(Private Parts (1997),4.0)
*/
topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)
/*
(To Die For (1995),5.931851273771102)
(Usual Suspects, The (1995),5.582301095666215)
(Dazed and Confused (1993),5.516272981542168)
(Clerks (1994),5.458065302395629)
(Secret Garden, The (1993),5.449949837103569)
(Amistad (1997),5.348768847643657)
(Being There (1979),5.30832117499004)
(Citizen Kane (1941),5.278933936827717)
(Reservoir Dogs (1992),5.250959077906759)
(Fantasia (1940),5.169863417126231)
*/

/* Compute item-to-item similarities between an item and the other items */
import org.jblas.DoubleMatrix
val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))
// aMatrix: org.jblas.DoubleMatrix = [1.000000; 2.000000; 3.000000]

/* Compute the cosine similarity between two vectors */
def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
	vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
}
// cosineSimilarity: (vec1: org.jblas.DoubleMatrix, vec2: org.jblas.DoubleMatrix)Double
val itemId = 567
val itemFactor = model.productFeatures.lookup(itemId).head
// itemFactor: Array[Double] = Array(0.15179359424040248, -0.2775955241896113, 0.9886005994661484, ... 
val itemVector = new DoubleMatrix(itemFactor)
// itemVector: org.jblas.DoubleMatrix = [0.151794; -0.277596; 0.988601; -0.464013; 0.188061; 0.090506; ... 
cosineSimilarity(itemVector, itemVector)
// res113: Double = 1.0000000000000002
val sims = model.productFeatures.map{ case (id, factor) => 
	val factorVector = new DoubleMatrix(factor)
	val sim = cosineSimilarity(factorVector, itemVector)
	(id, sim)
}
val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
// sortedSims: Array[(Int, Double)] = Array((567,1.0), (672,0.483244928887981), (1065,0.43267674923450905), ... 
println(sortedSims.mkString("\n"))
/* 
(567,1.0000000000000002)
(1471,0.6932331537649621)
(670,0.6898690594544726)
(201,0.6897964975027041)
(343,0.6891221044611473)
(563,0.6864214133620066)
(294,0.6812075443259535)
(413,0.6754663844488256)
(184,0.6702643811753909)
(109,0.6594872765176396)
*/ 

/* We can check the movie title of our chosen movie and the most similar movies to it */
println(titles(itemId))
// Wes Craven's New Nightmare (1994)
val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
sortedSims2.slice(1, 11).map{ case (id, sim) => (titles(id), sim) }.mkString("\n")
/* 
(Hideaway (1995),0.6932331537649621)
(Body Snatchers (1993),0.6898690594544726)
(Evil Dead II (1987),0.6897964975027041)
(Alien: Resurrection (1997),0.6891221044611473)
(Stephen King's The Langoliers (1995),0.6864214133620066)
(Liar Liar (1997),0.6812075443259535)
(Tales from the Crypt Presents: Bordello of Blood (1996),0.6754663844488256)
(Army of Darkness (1993),0.6702643811753909)
(Mystery Science Theater 3000: The Movie (1996),0.6594872765176396)
(Scream (1996),0.6538249646863378)
*/

/* Compute squared error between a predicted and actual rating */
// We'll take the first rating for our example user 789
val actualRating = moviesForUser.take(1)(0)
// actualRating: Seq[org.apache.spark.mllib.recommendation.Rating] = WrappedArray(Rating(789,1012,4.0))
val predictedRating = model.predict(789, actualRating.product)
// ...
// 14/04/13 13:01:15 INFO SparkContext: Job finished: lookup at MatrixFactorizationModel.scala:46, took 0.025404 s
// predictedRating: Double = 4.001005374200248
val squaredError = math.pow(predictedRating - actualRating.rating, 2.0)
// squaredError: Double = 1.010777282523947E-6

/* Compute Mean Squared Error across the dataset */
// Below code is taken from the Apache Spark MLlib guide at: http://spark.apache.org/docs/latest/mllib-guide.html#collaborative-filtering-1
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
// ...
// 14/04/13 15:29:21 INFO SparkContext: Job finished: count at <console>:31, took 0.538683 s
// Mean Squared Error = 0.08231947642632856
val RMSE = math.sqrt(MSE)
println("Root Mean Squared Error = " + RMSE)
// Root Mean Squared Error = 0.28691370902473196

/* Compute Mean Average Precision at K */

/* Function to compute average precision given a set of actual and predicted ratings */
// Code for this function is based on: https://github.com/benhamner/Metrics
def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
  val predK = predicted.take(k)
  var score = 0.0
  var numHits = 0.0
  for ((p, i) <- predK.zipWithIndex) {
    if (actual.contains(p)) {
      numHits += 1.0
      score += numHits / (i.toDouble + 1.0)
    }
  }
  if (actual.isEmpty) {
    1.0
  } else {
    score / scala.math.min(actual.size, k).toDouble
  }
}
val actualMovies = moviesForUser.map(_.product)
// actualMovies: Seq[Int] = ArrayBuffer(1012, 127, 475, 93, 1161, 286, 293, 9, 50, 294, 181, 1, 1008, 508, 284, 1017, 137, 111, 742, 248, 249, 1007, 591, 150, 276, 151, 129, 100, 741, 288, 762, 628, 124)
val predictedMovies = topKRecs.map(_.product)
// predictedMovies: Array[Int] = Array(27, 497, 633, 827, 602, 849, 401, 584, 1035, 1014)
val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)
// apk10: Double = 0.0

/* Compute recommendations for all users */
val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
val itemMatrix = new DoubleMatrix(itemFactors)
println(itemMatrix.rows, itemMatrix.columns)
// (1682,50)

// broadcast the item factor matrix
val imBroadcast = sc.broadcast(itemMatrix)

// compute recommendations for each user, and sort them in order of score so that the actual input 
// for the APK computation will be correct
val allRecs = model.userFeatures.map{ case (userId, array) => 
  val userVector = new DoubleMatrix(array)
  val scores = imBroadcast.value.mmul(userVector)
  val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
  val recommendedIds = sortedWithId.map(_._2 + 1).toSeq
  (userId, recommendedIds)
}

// next get all the movie ids per user, grouped by user id
val userMovies = ratings.map{ case Rating(user, product, rating) => (user, product) }.groupBy(_._1)
// userMovies: org.apache.spark.rdd.RDD[(Int, Seq[(Int, Int)])] = MapPartitionsRDD[277] at groupBy at <console>:21

// finally, compute the APK for each user, and average them to find MAPK
val K = 10
val MAPK = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) => 
  val actual = actualWithIds.map(_._2).toSeq
  avgPrecisionK(actual, predicted, K)
}.reduce(_ + _) / allRecs.count
println("Mean Average Precision at K = " + MAPK)
// Mean Average Precision at K = 0.030486963254725705

/* Using MLlib built-in metrics */

// MSE, RMSE and MAE
import org.apache.spark.mllib.evaluation.RegressionMetrics
val predictedAndTrue = ratingsAndPredictions.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
val regressionMetrics = new RegressionMetrics(predictedAndTrue)
println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
// Mean Squared Error = 0.08231947642632852
// Root Mean Squared Error = 0.2869137090247319

// MAPK
import org.apache.spark.mllib.evaluation.RankingMetrics
val predictedAndTrueForRanking = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) => 
  val actual = actualWithIds.map(_._2)
  (predicted.toArray, actual.toArray)
}
val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)
// Mean Average Precision = 0.07171412913757183

// Compare to our implementation, using K = 2000 to approximate the overall MAP
val MAPK2000 = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) => 
  val actual = actualWithIds.map(_._2).toSeq
  avgPrecisionK(actual, predicted, 2000)
}.reduce(_ + _) / allRecs.count
println("Mean Average Precision = " + MAPK2000)
// Mean Average Precision = 0.07171412913757186
