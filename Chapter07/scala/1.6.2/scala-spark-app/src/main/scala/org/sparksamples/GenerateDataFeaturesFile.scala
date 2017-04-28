package org.sparksamples

import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
 * A simple Spark app in Scala
 */
object GenerateDataFeaturesFile{

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def main(args: Array[String]) {
    val sc = Util.sc
    // we take the raw data in CSV format and convert it into a set of records
    // of the form (user, product, price)
    val rawData = sc.textFile("../data/hour_noheader.csv")
    val numData = rawData.count()

    val records = rawData.map(line => line.split(","))
    val first = records.first()

    println(numData.toInt)
    records.cache()
    print("Mapping of first categorical feature column: " +  get_mapping(records, 2))
    print("Mapping of second categorical feature column: " +  get_mapping(records, 3))
    var list = new ListBuffer[Map[String, Long]]()
    for( i <- 2 to 9){
      val m = get_mapping(records, i)
      list += m
    }
    val mappings = list.toList
    var catLen = 0
    mappings.foreach( m => (catLen +=m.size))

    val numLen = records.first().slice(11, 15).size
    val totalLen = catLen + numLen

    print("Feature vector length for categorical features:"+ catLen)
    print("Feature vector length for numerical features:" + numLen)
    print("Total feature vector length: " + totalLen)

    val data = {
      records.map(r => Util.extractLabel(r) + "," +  Util.extractSumFeature(r, catLen, mappings))
    }

    val data_collection = data.collect()
    val d_iterator = data_collection.iterator
    while(d_iterator.hasNext) {
      val x = d_iterator.next
      println(x)
    }
    val first_point = data.first()
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
    val date = format.format(new java.util.Date())
    data.saveAsTextFile("./output/x_features" + date + ".csv")

    sc.stop()
  }

}
