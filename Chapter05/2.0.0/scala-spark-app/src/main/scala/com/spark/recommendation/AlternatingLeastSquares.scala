package com.spark.recommendation

import org.apache.commons.math3.linear._
import org.apache.spark.sql.SparkSession

/**
  * Created by manpreet.singh on 04/09/16.
  *
  * Matrix factorization assumes that:
  * 1. Each user can be described by k attributes or features.
  *    For example, feature 1 might be a number that says how much each user likes sci-fi movies.
  * 2. Each item (movie) can be described by an analagous set of k attributes or features.
  *    To correspond to the above example, feature 1 for the movie might be a number that says how close the movie is to pure sci-fi.
  * 3. If we multiply each feature of the user by the corresponding feature of the movie and add everything together,
  *    this will be a good approximation for the rating the user would give that movie.
  */

object AlternatingLeastSquares {

  var movies = 0
  var users = 0
  var features = 0
  var ITERATIONS = 0
  val LAMBDA = 0.01 // Regularization coefficient

  private def vector(n: Int): RealVector =
    new ArrayRealVector(Array.fill(n)(math.random))

  private def matrix(rows: Int, cols: Int): RealMatrix =
    new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))

  def rSpace(): RealMatrix = {
    val mh = matrix(movies, features)
    val uh = matrix(users, features)
    mh.multiply(uh.transpose())
  }

  def rmse(targetR: RealMatrix, ms: Array[RealVector], us: Array[RealVector]): Double = {
    val r = new Array2DRowRealMatrix(movies, users)
    for (i <- 0 until movies; j <- 0 until users) {
      r.setEntry(i, j, ms(i).dotProduct(us(j)))
    }
    val diffs = r.subtract(targetR)
    var sumSqs = 0.0
    for (i <- 0 until movies; j <- 0 until users) {
      val diff = diffs.getEntry(i, j)
      sumSqs += diff * diff
    }
    math.sqrt(sumSqs / (movies.toDouble * users.toDouble))
  }

  def update(i: Int, m: RealVector, us: Array[RealVector], R: RealMatrix) : RealVector = {
    val U = us.length
    val F = us(0).getDimension
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    // For each user that rated the movie
    for (j <- 0 until U) {
      val u = us(j)
      // Add u * u^t to XtX
      XtX = XtX.add(u.outerProduct(u))
      // Add u * rating to Xty
      Xty = Xty.add(u.mapMultiply(R.getEntry(i, j)))
    }
    // Add regularization coefs to diagonal terms
    for (d <- 0 until F) {
      XtX.addToEntry(d, d, LAMBDA * U)
    }
    // Solve it with Cholesky
      new CholeskyDecomposition(XtX).getSolver.solve(Xty)
  }

  def main(args: Array[String]) {

    //initialize variables for rand data generation
    movies = 100
    users = 500
    features = 10
    ITERATIONS = 5
    var slices = 2

    //initiate Spack Context

    val spark = SparkSession.builder.master("local[2]").appName("AlternatingLeastSquares").getOrCreate()
    val sc = spark.sparkContext

    // create a Realmatrix with the follow
    // movies matrix : 100 x 10
    // feature matrix : 500 x 10
    // populate with random numbers
    // multiple movie matrix with transpose of user matric
    // (100 x 10 ) x ( 10 x 500) = 100 x 500 matrix

    val r_space = rSpace()
    println("No of rows:" + r_space.getRowDimension)
    println("No of cols:" + r_space.getColumnDimension)

    // Initialize m and u randomly
    var ms = Array.fill(movies)(vector(features))
    var us = Array.fill(users)(vector(features))

    // Iteratively update movies then users
    val Rc = sc.broadcast(r_space)
    var msb = sc.broadcast(ms)
    var usb = sc.broadcast(us)
    //Objective is to find ms and us matrices by iterating over existing values and comparing with the real value matrix
    //Choksey decomposition is being used to solve this

    for (iter <- 1 to ITERATIONS) {
      println(s"Iteration $iter:")
      ms = sc.parallelize(0 until movies, slices)
        .map(i => update(i, msb.value(i), usb.value, Rc.value))
        .collect()
      msb = sc.broadcast(ms) // Re-broadcast ms because it was updated
      us = sc.parallelize(0 until users, slices)
        .map(i => update(i, usb.value(i), msb.value, Rc.value.transpose()))
        .collect()
      usb = sc.broadcast(us) // Re-broadcast us because it was updated
      println("RMSE = " + rmse(r_space, ms, us))
      println()
    }
    spark.stop()
  }
}
