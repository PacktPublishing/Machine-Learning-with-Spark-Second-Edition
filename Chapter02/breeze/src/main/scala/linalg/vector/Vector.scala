package linalg.vector

import breeze.linalg._
import breeze.stats.mean

/**
  * Created by manpreet.singh on 23/01/16.
  */
object Vector {

  def main(args: Array[String]) {
    // dense vector
    val dv = DenseVector(2f, 0f, 3f, 2f, -1f)
    dv.update(3, 6f)
    println(dv)

    // sparse vector
    val sv:SparseVector[Double] = SparseVector(5)()
    sv(0) = 1
    sv(2) = 3
    sv(4) = 5
    val ma:SparseVector[Double] = sv.mapActivePairs((i,x) => x+1)
    println(ma)

    // spark mllib support for local vector
    // Create a dense vector (1.0, 0.0, 3.0).
    //val dv1: Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
    //val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    //val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

    // vector operations (mul, dot)
    val a = DenseVector(0.56390, 0.36231, 0.14601, 0.60294, 0.14535)
    val b = DenseVector(0.15951, 0.83671, 0.56002, 0.57797, 0.54450)
    println(a.t * b)
    println(a dot b)

    // find mean
    val mn = mean(DenseVector(0.0,1.0,2.0))
    println(mn)

    // normalize
    val v = DenseVector(-0.4326, -1.6656, 0.1253, 0.2877, -1.1465)
    val nm = norm(v, 1)
    println(nm)

    // min-max
    val v1 = DenseVector(2, 0, 3, 2, -1)
    println(argmin(v1))
    println(argmax(v1))
    println(min(v1))
    println(max(v1))

    // to array
    val ar = DenseVector(1, 2, 3)
    println(ar.toArray)
    println(ar.data)

    // compare ops
    val a1 = DenseVector(1, 2, 3)
    val b1 = DenseVector(1, 4, 1)
    println((a1 :== b1))
    println((a1 :<= b1))
    println((a1 :>= b1))
    println((a1 :< b1))
    println((a1 :> b1))

    // sparse mean operation
    val svm = mean(SparseVector(0.0,1.0,2.0))
    val svm1 = mean(SparseVector(0.0,3.0))
    println(svm, svm1)

    // sparse mul operation
    val sva = SparseVector(0.56390,0.36231,0.14601,0.60294,0.14535)
    val svb = SparseVector(0.15951,0.83671,0.56002,0.57797,0.54450)
    println(sva.t * svb)
    println(sva dot svb)

    // all operations
    val x = DenseVector(-0.4326, -1.6656, 0.1253, 0.2877, -1.1465)
    // DenseVector(0, 0, 0, 0, 0)

    val m = DenseVector(0.56390, 0.36231, 0.14601, 0.60294, 0.14535)

    val r = DenseMatrix.rand(5,5)

//    println(m.t) // transpose
//    println(x + x) // addition
//    println(m * x) // multiplication by vector
//    println(m * 3) // by scalar
//    println(m * m) // by matrix
//    println(m :* m) // element wise mult, Matlab .*

  }

}
