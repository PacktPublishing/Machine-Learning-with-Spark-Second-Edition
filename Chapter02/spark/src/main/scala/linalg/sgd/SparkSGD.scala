package linalg.sgd
import scala.util.Random
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.GradientDescent
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.optimization.LogisticGradient
import org.apache.spark.SparkContext



object SparkSGD {
  def main(args: Array[String]): Unit = {
    val m = 4
    val n = 200000
    val sc = new SparkContext("local[2]", "")
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    val (weights, loss) = GradientDescent.runMiniBatchSGD(
      points,
      new LogisticGradient,
      new SquaredL2Updater,
      0.1,
      2,
      1.0,
      1.0,
      Vectors.dense(new Array[Double](n)))
    println("w:"  + weights(0))
    println("loss:" + loss(0))
    sc.stop()

  }
}