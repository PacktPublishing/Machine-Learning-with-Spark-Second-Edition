package org.utils

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by manpreet.singh on 14/12/16.
  */
object StandaloneSpark {

  private val spark = SparkSession
    .builder
    .appName("AIApps")
    .master("local[1]")
    .getOrCreate()


  // get spark instance
  def getSparkInstance(): SparkSession = {
    return spark
  }

  // get sql context
  def getSQLContext(): SQLContext = {
    // get sql context here
    val sqlContext = spark.sqlContext
    return sqlContext
  }
}
