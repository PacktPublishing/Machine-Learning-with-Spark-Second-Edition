package org.sparksamples

/**
  * Created by Rajdeep on 12/22/15.
  * Modified for DataFrame on 9/25/16
  */

object MovieDataFillingBadValues {

  def main(args: Array[String]) {
    val movie_data_df = Util.getMovieDataDF()
    movie_data_df.createOrReplaceTempView("movie_data")
    movie_data_df.printSchema()

    Util.spark.udf.register("convertYear", Util.convertYear _)
    movie_data_df.show(false)

    val movie_years = Util.spark.sql("select convertYear(date) as year from movie_data")

    movie_years.createOrReplaceTempView("movie_years")
    Util.spark.udf.register("replaceEmptyStr", replaceEmptyStr _)

    val years_replaced =  Util.spark.sql("select replaceEmptyStr(year) as r_year from movie_years")

    val movie_years_filtered = movie_years.filter(x =>(x == 1900) )
    val years_filtered_valid = years_replaced.filter(x => (x != 1900)).collect()
    val years_filtered_valid_int = new Array[Int](years_filtered_valid.length)
    for( i <- 0 until years_filtered_valid.length -1){
      val x = Integer.parseInt(years_filtered_valid(i)(0).toString)
      years_filtered_valid_int(i) = x
    }
    val years_filtered_valid_int_sorted = years_filtered_valid_int.sorted

    val years_replaced_int = new Array[Int](years_replaced.collect().length)

    val years_replaced_collect = years_replaced.collect()

    for( i <- 0 until years_replaced.collect().length -1){
      val x = Integer.parseInt(years_replaced_collect(i)(0).toString)
      years_replaced_int(i) = x
    }

    val years_replaced_rdd = Util.sc.parallelize(years_replaced_int)


    val num = years_filtered_valid.length
    var sum_y = 0
    years_replaced_int.foreach(sum_y += _)
    println("Total sum of Entries:" + sum_y)
    println("Total No of Entries:" + num)
    val mean = sum_y/num
    val median_v = median(years_filtered_valid_int_sorted)
    Util.sc.broadcast(mean)
    println("Mean value of Year:" + mean)
    println("Median value of Year:" + median_v)
    val years_x = years_replaced_rdd.map(v => replace(v , median_v))
    println("Total Years after conversion:" + years_x.count())
    var count = 0
    Util.sc.broadcast(count)
    val years_with1900 = years_x.map(x => (if(x == 1900) {count +=1}))
    println("Count of 1900: " + count)
  }

  def replace(v : Int, median : Int): Unit = {
    if (v == 1900) {
      return median
    } else {
      return v
    }
  }

  def replaceEmptyStr(v : Int): Int = {
    try {
      if (v.equals("") ) {
        return 1900
      } else {

        return v
      }
    }catch {
      case e: Exception => println(e)
        return 1900
    }
  }

  def median(input :Array[Int]) : Int = {
    val l = input.length
    val l_2 = l/2.toInt
    val x = l%2
    var y = 0
    if(x == 0) {
      y = ( input(l_2) + input(l_2 + 1))/2
    }else {
      y = (input(l_2))
    }
    return y
  }
}