package org.sparksamples

import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * LogisticalRegression App
  * @author Rajdeep Dua
  */
object CalculateStdDeviation{

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def main(args: Array[String]) {

    val recordsArray = Util.getRecords()
    val records = recordsArray._1
    val first = records.first()
    val numData = recordsArray._2

    println(numData.toString())
    records.cache()
    print("Mapping of first categorical feature column: " +  get_mapping(records, 2))
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
      records.map(r => Util.extractFeatures(r, catLen, mappings))
    }
    //data.saveAsTextFile("./output/temp.txt")
    val count_columns = data.first().size

    var a = 0;
    var x = new Array[Double](count_columns)
    // for loop execution with a range
    for( a <- 0 to (count_columns -1) ){
      val stddev = data.map(r => r(a)).stdev()
      //println(a +  ": " +  );
      x.update(a,stddev)

    }
    for( a <- 0 to (count_columns -1) ){
      println(a  + " : " + x(a))
    }

    //val data_1_std_dev = data.map(r => r(1)).stdev()
    //println(data_1_std_dev)


  }

}
