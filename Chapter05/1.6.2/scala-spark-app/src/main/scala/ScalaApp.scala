import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}

//import org.apache.spark.
/**
 * A simple Spark app in Scala
 */
object ScalaApp {

  val PATH = "../../data"
  def main(args: Array[String]) {

    //val file = new File("output-" + new Util().getDate() + ".log")
    //val bw = new BufferedWriter(new FileWriter(file))
    val sc = new SparkContext("local[2]", "Chapter 5 App")
    val rawData = sc.textFile(PATH + "/ml-100k/u.data")
    rawData.first()
    // 14/03/30 13:21:25 INFO SparkContext: Job finished: first at <console>:17, took 0.002843 s
    // res24: String = 196	242	3	881250949

    /* Extract the user id, movie id and rating only from the dataset */
    val rawRatings = rawData.map(_.split("\t").take(3))
    rawRatings.first()
    // 14/03/30 13:22:44 INFO SparkContext: Job finished: first at <console>:21, took 0.003703 s
    // res25: Array[String] = Array(196, 242, 3)

    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    val ratingsFirst = ratings.first()
    println(ratingsFirst)

    /* Train the ALS model with rank=50, iterations=10, lambda=0.01 */
    val model = ALS.train(ratings, 50, 10, 0.01)

    /* Inspect the user factors */
    println(model.userFeatures)
    /* Count user factors and force computation */
    println(model.userFeatures.count)
    println(model.productFeatures.count)

    /* Make a prediction for a single user and movie pair */
    val predictedRating = model.predict(789, 123)
    println(predictedRating)
    val userId = 789
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
    println(topKRecs.mkString("\n"))

    val movies = sc.textFile(PATH + "/ml-100k/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    titles(123)
    // res68: String = Frighteners, The (1996)
    val moviesForUser = ratings.keyBy(_.user).lookup(789)
    // moviesForUser: Seq[org.apache.spark.mllib.recommendation.Rating] = WrappedArray(Rating(789,1012,4.0), Rating(789,127,5.0), Rating(789,475,5.0), Rating(789,93,4.0), ...
    // ...
    println(moviesForUser.size)
    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)
    sc.stop()
    //bw.close()
  }

  class Util {
    def getDate(): String = {
      val today = Calendar.getInstance().getTime()
      // (2) create a date "formatter" (the date format we want)
      val formatter = new SimpleDateFormat("yyyy-MM-dd-hh.mm.ss")
   
      // (3) create a new String using the date format we want
      val folderName = formatter.format(today)
      return folderName
    }
  }

}
