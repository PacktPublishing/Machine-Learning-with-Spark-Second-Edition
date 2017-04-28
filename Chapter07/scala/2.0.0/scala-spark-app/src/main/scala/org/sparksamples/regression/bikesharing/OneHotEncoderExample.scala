package org.sparksamples.regression.bikesharing

import org.apache.spark.sql.SparkSession

/**
  * Created by manpreet.singh on 14/11/16.
  */
object OneHotEncoderExample {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example").master("local[1]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    val df = spark.createDataFrame(Seq(
      (0, 3),
      (1, 2),
      (2, 4),
      (3, 3),
      (4, 3),
      (5, 4)
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
    val encoded = encoder.transform(indexed)
    encoded.select("id", "categoryVec").show()
  }

}
