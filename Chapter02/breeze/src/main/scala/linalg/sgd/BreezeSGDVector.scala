package linalg.sgd
import breeze.linalg._
import breeze.optimize._

object BreezeSGDVector {
  def main(args: Array[String]): Unit = {
    val sgd = StochasticGradientDescent[DenseVector[Double]](2.0,100)
    val init = DenseVector[Double]()
    val f = new BatchDiffFunction[DenseVector[Double]] {
        def calculate(x: DenseVector[Double], r: IndexedSeq[Int]) = {
          val r = x - 3.0
          ((r dot r), (x * 2.0) - 6.0)
        }
        val fullRange = 0 to 1
    }

    val result = sgd.minimize(f,init)
    println("result:" + result)
    norm(result :- DenseVector.ones[Double](init.size) * 3.0,2) < 1E-10



  }

}