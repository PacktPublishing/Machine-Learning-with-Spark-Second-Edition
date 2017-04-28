package linalg.operations

import breeze.linalg._
import breeze.numerics.{floor, ceil}

/**
  * Created by manpreet.singh on 28/01/16.
  */
object common {

  // vector's
  val v1 = DenseVector(3, 7, 8.1, 4, 5)
  val v2 = DenseVector(1, 9, 3, 2.3, 8)

  // column matrix (3 * 3)
  val m1 = DenseMatrix((2, 3, 1), (4, 5, 1), (6, 7, 1))
  val m2 = DenseMatrix((3, 4, 1), (2, 6, 1), (8, 2, 1))

  // elementwise add operation
  def add(): Unit = {
    println(v1 + v2)
    println(m1 + m2)
  }

  // elementwise mul operation
  def mul(): Unit = {
    println(v1 :* v2)
    println(m1 :* m2)
  }

  // elementwise compare operation
  def compare(): Unit = {
    println(v1 :< v2)
    println(m1 :< m2)
  }

  // inplace addition operation
//  def inplace(): Unit = {
//    println(v1 += 2)
//    println(m1 += 3)
//  }

  // vector dot product operation
  def dot(): Unit = {
    println(v1 dot v2)
  }

  // elementwise sum
  def sumof(): Unit = {
    println(sum(v1))
  }

  // elementwise max
  def maxElement(): Unit = {
    println(max(v1))
    println(max(m1))
  }

  // elementwise argmax
  def argmaxElement(): Unit = {
    println(argmax(v1))
    println(argmax(m1))
  }

  // ceiling (Rounds numbers to the next highest integer)
  def ceiling(): Unit = {
    println(ceil(v1))
    println(ceil(m1))
  }

  // floor (Rounds numbers to the next lowest integer.)
  def flooring(): Unit = {
    println(floor(v1))
    println(floor(m1))
  }

  def main(args: Array[String]) {
    add()
    mul()
    compare()
    dot()
    sumof()
    maxElement()
    argmaxElement()
    ceiling()
    flooring()
  }
}
