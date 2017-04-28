package org.sparksamples.classification

import org.apache.spark.SparkContext
import org.sparksamples.classification.stumbleupon.SparkConstants

/**
	* @author Rajdeep dua
	*         
	*/

object DataPersistenceApp {

	def main(args: Array[String]) {
		val sc = new SparkContext("local[1]", "Classification")

		// get StumbleUpon dataset 'https://www.kaggle.com/c/stumbleupon'
		val records = sc.textFile(SparkConstants.PATH + "data/train_noheader.tsv").map(line => line.split("\t"))

    val data_persistent = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      val len = features.size.toInt
      val len_2 = math.floor(len/2).toInt
      val x = features.slice(0,len_2)

      val y = features.slice(len_2 -1 ,len )
      var i=0
      var sum_x = 0.0
      var sum_y = 0.0
      while (i < x.length) {
        sum_x += x(i)
        i += 1
      }
      i = 0
      while (i < y.length) {
        sum_y += y(i)
        i += 1
      }
      if (sum_y != 0.0) {
        if(sum_x != 0.0) {
          math.log(sum_x) + "," + math.log(sum_y)
        }else {
          sum_x + "," + math.log(sum_y)
        }

      }else {
        if(sum_x != 0.0) {
          math.log(sum_x) + "," + 0.0
        }else {
          sum_x + "," + 0.0
        }
      }

    }
    val dataone = data_persistent.first()
    data_persistent.saveAsTextFile(SparkConstants.PATH + "/results/raw-input-log")
		sc.stop()

	}

}