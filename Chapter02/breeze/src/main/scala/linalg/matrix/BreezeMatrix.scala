package linalg.matrix

import breeze.linalg.{DenseMatrix, DenseVector}


object BreezeMatrix {

  def main(args: Array[String]) {
    val a = DenseMatrix((1,2),(3,4))
    println("a : \n" + a)
    val m = DenseMatrix.zeros[Int](5,5)
    println("Created a 5x5 matrix\n" + m )
    println( "m.rows :" + m.rows + " m.cols : "  + m.cols)
    println("First Column of m : \n" + m(::,1))

    m(4,::) := DenseVector(5,5,5,5,5).t
    println("Assigned 5,5,5,5,5 to last row of m.\n")
    println(m)

  }

}
