import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ SparseVector => SV }

/**
 * A simple Spark app in Scala
 */
object DocumentClassification {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")

    val path = "../data/20news-bydate-train/*"
    val rdd = sc.wholeTextFiles(path)
    val text = rdd.map { case (file, text) => text }
    val newsgroups = rdd.map { case (file, text) => file.split("/").takeRight(2).head }
    val newsgroupsMap = newsgroups.distinct.collect().zipWithIndex.toMap
    val dim = math.pow(2, 18).toInt
    val hashingTF = new HashingTF(dim)

    var tokens = text.map(doc => TFIDFExtraction.tokenize(doc))
    val tf = hashingTF.transform(tokens)
    tf.cache
    val v = tf.first.asInstanceOf[SV]


    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    val zipped = newsgroups.zip(tfidf)
    val train = zipped.map { case (topic, vector) => LabeledPoint(newsgroupsMap(topic), vector) }
    train.cache
    val model = NaiveBayes.train(train, lambda = 0.1)

    val testPath = "../data/20news-bydate-test/*"
    val testRDD = sc.wholeTextFiles(testPath)
    val testLabels = testRDD.map { case (file, text) =>
      val topic = file.split("/").takeRight(2).head
      newsgroupsMap(topic)
    }
    val testTf = testRDD.map { case (file, text) => hashingTF.transform(TFIDFExtraction.tokenize(text)) }
    val testTfIdf = idf.transform(testTf)
    val zippedTest = testLabels.zip(testTfIdf)
    val test = zippedTest.map { case (topic, vector) => LabeledPoint(topic, vector) }

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(accuracy)
    // Updated Dec 2016 by Rajdeep
    //0.7928836962294211
    val metrics = new MulticlassMetrics(predictionAndLabel)
    println(metrics.weightedFMeasure)
    //0.7822644376431702

    val rawTokens = rdd.map { case (file, text) => text.split(" ") }
    val rawTF = rawTokens.map(doc => hashingTF.transform(doc))
    val rawTrain = newsgroups.zip(rawTF).map { case (topic, vector) => LabeledPoint(newsgroupsMap(topic), vector) }
    val rawModel = NaiveBayes.train(rawTrain, lambda = 0.1)
    val rawTestTF = testRDD.map { case (file, text) => hashingTF.transform(text.split(" ")) }
    val rawZippedTest = testLabels.zip(rawTestTF)
    val rawTest = rawZippedTest.map { case (topic, vector) => LabeledPoint(topic, vector) }
    val rawPredictionAndLabel = rawTest.map(p => (rawModel.predict(p.features), p.label))
    val rawAccuracy = 1.0 * rawPredictionAndLabel.filter(x => x._1 == x._2).count() / rawTest.count()
    println(rawAccuracy)
    // 0.7661975570897503
    val rawMetrics = new MulticlassMetrics(rawPredictionAndLabel)
    println(rawMetrics.weightedFMeasure)
    // older value 0.7628947184990661
    // dec 2016 : 0.7653320418573546
    sc.stop()
  }

}
