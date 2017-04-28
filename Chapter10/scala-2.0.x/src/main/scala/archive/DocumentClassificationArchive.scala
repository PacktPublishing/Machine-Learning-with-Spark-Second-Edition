package archive

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.classification.NaiveBayes
//import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{SparseVector => SV2}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * A simple Spark app in Scala
 */
object DocumentClassificationArchive {

  case class MyClass(label: Double, text: String)
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder()
      .appName("SparkRatingData").config(spConfig)
      .getOrCreate()
    import spark.implicits._

    val path = "../data/20news-bydate-sample/*"
    val rdd = sc.wholeTextFiles(path)
    val text = rdd.map { case (file, text) => text }
    val newsgroups = rdd.map { case (file, text) => file.split("/").takeRight(2).head }
    println(newsgroups.first())
    val newsgroupsMap = newsgroups.distinct.collect().zipWithIndex.toMap
    sc.broadcast(newsgroupsMap)

    val textModified = rdd.map { case (file, text) =>
      {
        val labelText = file.split("/").takeRight(2).head
        val label = newsgroupsMap.get(labelText).get
        val y = (label.toDouble, text)
        y
      }
    }

    val df = textModified.map(
      r => MyClass(r._1, r._2)).toDF("label","text")
    df.show()


    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(df)
    wordsData.show()
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData).select("label", "rawFeatures")

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    //println(zippedTest.first())

    //val rescaledLabelPoint = zippedTest
    val trainingData = rescaledData.select("label","features").rdd.map( row => {
      println(row.getDouble(0))
      println(row)
      println(row(1))
      LabeledPoint(row.getDouble(0), {
        val x = row(1).asInstanceOf[SV2]
        val xDense = x.toDense
        val a  = xDense.toArray
        val b = new  DenseVector(a)
        b
      })
    })

    val testData = getRescaledTestData(sc, spark)
    val model = NaiveBayes.train(trainingData, lambda = 0.01)

    val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()
    print(accuracy)

    val metrics = new MulticlassMetrics(predictionAndLabel)
    println(metrics.accuracy)
    println(metrics.weightedFalsePositiveRate)
    println(metrics.weightedPrecision)
    println(metrics.weightedFMeasure)
    println(metrics.weightedRecall)

    sc.stop()
  }

  def getRescaledTestData(sc : SparkContext, spark : SparkSession) : RDD[LabeledPoint] = {
    val path = "../data/20news-bydate-test/*"
    val rdd = sc.wholeTextFiles(path)
    val text = rdd.map { case (file, text) => text }
    val newsgroups = rdd.map { case (file, text) => file.split("/").takeRight(2).head }
    println(newsgroups.first())
    val newsgroupsMap = newsgroups.distinct.collect().zipWithIndex.toMap
    sc.broadcast(newsgroupsMap)

    val textModified = rdd.map {
      case (file, text) => {
        val labelText = file.split("/").takeRight(2).head
        val label = newsgroupsMap.get(labelText).get
        val y = (label.toDouble, text)
        y
      }
    }
    import spark.implicits._
    val df = textModified.map(
      r => MyClass(r._1, r._2)).toDF("label","text")

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(df)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    val rescaledLabelPoint = rescaledData.select("label", "features").rdd.
      map( row => LabeledPoint(row.getDouble(0), {
        val x = row(1).asInstanceOf[SV2]
        val xDense = x.toDense
        val a  = xDense.toArray
        val b = new  DenseVector(a)
        b
      }))

    return rescaledLabelPoint
  }
}
