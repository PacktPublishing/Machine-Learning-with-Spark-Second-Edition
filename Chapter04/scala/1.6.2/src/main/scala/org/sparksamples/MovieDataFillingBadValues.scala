package org.sparksamples

/**
  * Created by Rajdeep on 12/22/15.
  */

object MovieDataFillingBadValues {

  def main(args: Array[String]) {
    val movie_data = Util.getMovieData()
    val movie_fields = movie_data.map(lines =>  lines.split("\\|"))
    val years = movie_fields.map( field => field(2)).map( x => Util.convertYear(x))
    val years_replaced = years.map(x => replaceEmptyStr(x))

    val years_filtered = years.filter(x =>(x == 1900) )
    val years_filtered_valid = years_replaced.filter(x => (x != 1900)).collect()

    val num = years_filtered_valid.length
    var sum_y = 0
    years_replaced.collect().foreach(sum_y += _)
    println("Total sum of Entries:" + sum_y)
    println("Total No of Entries:" + num)
    val mean = sum_y/num
    val years_filtered_valid_sorted = years_filtered_valid.sorted
    val median_v = median(years_filtered_valid_sorted)
    Util.sc.broadcast(mean)
    println("Mean value of Year:" + mean)
    println("Median value of Year:" + median_v)
    val years_x = years_replaced.map(v =>   replace(v , median_v))
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