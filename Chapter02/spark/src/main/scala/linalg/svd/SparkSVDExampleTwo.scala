package linalg.svd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object SparkSVDExampleTwo extends App {

  val spConfig = (new SparkConf).setMaster("local").setAppName("SparkSVDDemo")
  val sc = new SparkContext(spConfig)


  val rows = sc.textFile("./svd.txt").map { line =>
    val values = line.split(' ').map(_.toDouble)
    Vectors.dense(values)
  }

  val mat = new RowMatrix(rows)

  // Compute SVD
  val svd = mat.computeSVD(mat.numCols().toInt, computeU = true)
  val U: RowMatrix = svd.U
  val s: Vector = svd.s
  val V: Matrix = svd.V

  println("Left Singular vectors :")
  U.rows.foreach(println)

  println("Singular values are :")
  println(s)

  println("Right Singular vectors :")
  println(V)

  sc.stop
}
