

package org.sparksamples.lda

// scalastyle:off println
// $example on$
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.SparkSession
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example demonstrating LDA.
  * Run with
  * {{{
  * bin/run-example ml.LDAExample
  * }}}
  */
object LDAExample {

  val SPARK_PATH = "/home/ubuntu/work/spark-2.0.0-bin-hadoop2.7/";
  // Creates a SparkSession
  /*val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()*/

  val spConfig = (new SparkConf).setMaster("local[1]").setAppName("SparkApp").
    set("spark.driver.allowMultipleContexts", "true")

  val spark = SparkSession
    .builder()
    .appName("Spark SQL Example")
    .config(spConfig)
    .getOrCreate()

  // $example on$
  // Loads data.
  val dataset = spark.read.format("libsvm")
    .load(SPARK_PATH + "data/mllib/sample_lda_libsvm_data.txt")

  // Trains a LDA model.
  val lda = new LDA().setK(10).setMaxIter(10)
  val model = lda.fit(dataset)

  val ll = model.logLikelihood(dataset)
  val lp = model.logPerplexity(dataset)
  println(s"The lower bound on the log likelihood of the entire corpus: $ll")
  println(s"The upper bound bound on perplexity: $lp")

  // Describe topics.
  val topics = model.describeTopics(3)
  println("The topics described by their top-weighted terms:")
  topics.show(false)

  // Shows the result.
  val transformed = model.transform(dataset)
  transformed.show(false)
  // $example off$

  spark.stop()

}
// scalastyle:on println
