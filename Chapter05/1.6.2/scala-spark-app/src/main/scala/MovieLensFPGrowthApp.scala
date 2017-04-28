import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.recommendation.Rating

import scala.collection.mutable.ListBuffer

/**
 * A simple Spark app in Scala
 */
object MovieLensFPGrowthApp {

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

    val userId = 789
    val K = 10

    val movies = sc.textFile(PATH + "/ml-100k/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    titles(123)

    var eRDD = sc.emptyRDD
    var z = Seq[String]()

    val l = ListBuffer()
    val aj = new Array[String](100)
    var i = 0
    for( a <- 801 to 900) {
      val moviesForUserX = ratings.keyBy(_.user).lookup(a)
      val moviesForUserX_10 = moviesForUserX.sortBy(-_.rating).take(10)
      val moviesForUserX_10_1 = moviesForUserX_10.map(r => r.product)
      var temp = ""
      for( x <- moviesForUserX_10_1){
        temp = temp + " " + x
        println(temp)

      }

      aj(i) = temp
      i += 1
    }
    z = aj
    val transaction2 = z.map(_.split(" "))

    val rddx = sc.parallelize(transaction2, 2).cache()

    val fpg = new FPGrowth()
    val model6 = fpg
      .setMinSupport(0.1)
      .setNumPartitions(1)
      .run(rddx)

    model6.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    sc.stop()
  }

}
