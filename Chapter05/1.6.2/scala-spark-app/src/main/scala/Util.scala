/**
  * Created by ubuntu on 3/16/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
  * Created by Rajdeep Dua on 2/2/16.
  */
object Util {
  val PATH = "../.."
  val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
  val sc = new SparkContext(spConfig)

  def getMovieData() : RDD[String] = {
    val movie_data = sc.textFile(PATH + "/data/ml-100k/u.item")
    return movie_data
  }
}