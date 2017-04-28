package org.stumbleuponclassifier

import org.apache.spark.SparkConf

/**
  * Created by manpreet.singh on 26/04/16.
  */
object SparkCommonUtils {

  def createSparkConf(appName: String): SparkConf = {
     new SparkConf().setAppName(appName).setMaster(SparkConstants.SparkMaster)
  }

}
