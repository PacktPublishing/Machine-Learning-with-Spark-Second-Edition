package org.sparksamples

import breeze.linalg._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Rajdeep Dua
  */
object ImageProcessing {
  val PATH= "../../data"

  def main(args: Array[String]): Unit = {
    val spConfig = (new SparkConf).setMaster("local[1]").setAppName("SparkApp").
      set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(spConfig)
    val path = PATH +  "/lfw/*"
    val rdd = sc.wholeTextFiles(path)
    val first = rdd.first
    val files = rdd.map { case (fileName, content) => fileName.replace("file:", "") }

    println(files.count)

    val aePath = PATH + "/lfw/Aaron_Eckhart/Aaron_Eckhart_0001.jpg"
    val aeImage = Util.loadImageFromFile(aePath)

    val grayImage = Util.processImage(aeImage, 100, 100)
    import java.io.File
    import javax.imageio.ImageIO
    ImageIO.write(grayImage, "jpg", new File("/tmp/aeGray.jpg"))


    val pixels = files.map(f => Util.extractPixels(f, 50, 50))
    println(pixels.take(10).map(_.take(10).mkString("", ",", ", ...")).mkString("\n"))

    val vectors = pixels.map(p => Vectors.dense(p))
    // the setName method createa a human-readable name that is displayed in the Spark Web UI
    vectors.setName("image-vectors")
    // remember to cache the vectors to speed up computation
    vectors.cache
    val scaler = new StandardScaler(withMean = true, withStd = false).fit(vectors)
    val scaledVectors = vectors.map(v => scaler.transform(v))
    val matrix = new RowMatrix(scaledVectors)
    val K = 10
    val pc = matrix.computePrincipalComponents(K)
    val rows = pc.numRows
    val cols = pc.numCols
    val pcBreeze = new DenseMatrix(rows, cols, pc.toArray)
    println(rows, cols)
    csvwrite(new File(PATH + "/pc.csv"), pcBreeze)
    val projected = matrix.multiply(pc)
    println(projected.numRows, projected.numCols)
    // (1055,10)
    println(projected.rows.take(5).mkString("\n"))
    val svd = matrix.computeSVD(10, computeU = true)
    println(s"U dimension: (${svd.U.numRows}, ${svd.U.numCols})")
    println(s"S dimension: (${svd.s.size}, )")
    println(s"V dimension: (${svd.V.numRows}, ${svd.V.numCols})")
    // test the function
    println(approxEqual(Array(1.0, 2.0, 3.0), Array(1.0, 2.0, 3.0)))
    // true
    println(approxEqual(Array(1.0, 2.0, 3.0), Array(3.0, 2.0, 1.0)))
    // false
    println(approxEqual(svd.V.toArray, pc.toArray))
    val breezeS = breeze.linalg.DenseVector(svd.s.toArray)

    val projectedSVD = svd.U.rows.map { v =>
      val breezeV = breeze.linalg.DenseVector(v.toArray)
      val multV = breezeV :* breezeS
      Vectors.dense(multV.data)
    }
    projected.rows.zip(projectedSVD).map { case (v1, v2) => approxEqual(v1.toArray, v2.toArray) }.filter(b => true).count
    // 1055

    // inspect singular values
    val sValues = (1 to 5).map { i => matrix.computeSVD(i, computeU = false).s }
    sValues.foreach(println)
    /*
    [54091.00997110354]
    [54091.00997110358,33757.702867982436]
    [54091.00997110357,33757.70286798241,24541.193694775946]
    [54091.00997110358,33757.70286798242,24541.19369477593,23309.58418888302]
    [54091.00997110358,33757.70286798242,24541.19369477593,23309.584188882982,21803.09841158358]
    */
    val svd300 = matrix.computeSVD(300, computeU = false)
    val sMatrix = new DenseMatrix(1, 300, svd300.s.toArray)
    println(sMatrix)
    csvwrite(new File(PATH + "/s.csv"), sMatrix)

  }
  def approxEqual(array1: Array[Double], array2: Array[Double], tolerance: Double = 1e-6): Boolean = {
    // note we ignore sign of the principal component / singular vector elements
    val bools = array1.zip(array2).map { case (v1, v2) => if (math.abs(math.abs(v1) - math.abs(v2)) > 1e-6) false else true }
    bools.fold(true)(_ & _)
  }
}