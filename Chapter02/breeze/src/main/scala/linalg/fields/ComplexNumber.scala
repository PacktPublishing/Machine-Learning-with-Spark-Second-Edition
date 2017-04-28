package linalg.fields

import breeze.math.Complex

/**
  * Created by manpreet.singh on 27/01/16.
  */
object ComplexNumber {

  def complexNumbers(): Unit = {
    val i = Complex.i
    // add
    println((1 + 2 * i) + (2 + 3 * i))

    // sub
    println((1 + 2 * i) - (2 + 3 * i))

    // divide
    println((5 + 10 * i) / (3 - 4 * i))

    // mul
    println((1 + 2 * i) * (-3 + 6 * i))
    println((1 + 5 * i) * (-3 + 2 * i))

    // neg
    println(-(1 + 2 * i))

    // sum of complex numbers
    val x = List((5 + 7 * i), (1 + 3 * i), (13 + 17 * i))
    println(x.sum)

    // product of complex numbers
    val x1 = List((5 + 7 * i), (1 + 3 * i), (13 + 17 * i))
    println(x1.product)

    // sort list of complex numbers
    val x2 = List((5 + 7 * i), (1 + 3 * i), (13 + 17 * i))
    println(x2.sorted)

  }

  def main(args: Array[String]) {
    complexNumbers()
  }


}
