package org.sparksamples

/**
  * Created by Rajdeep on 12/22/15.
  */
import java.util.Date

import breeze.linalg.DenseVector

object RatingData {
  val user_data = Util.getUserData()
  def main(args: Array[String]) {
    val rating_data_raw = Util.sc.textFile("../../data/ml-100k/u.data")
    println(rating_data_raw.first())
    val num_ratings = rating_data_raw.count()
    println("num_ratings:" + num_ratings)
    val rating_data = rating_data_raw.map(line => line.split("\t"))

    val ratings = rating_data.map(fields => fields(2).toInt)
    val max_rating = ratings.reduce( (x, y) => math.max(x, y))
    val min_rating = ratings.reduce( (x, y) => math.min(x, y))
    val mean_rating = ratings.reduce( (x, y) => x + y) / num_ratings.toFloat
    println("max_rating: " + max_rating)
    println("min_rating: " + min_rating)
    println("mean_rating: " + mean_rating)

    println("user_data.first():"  + user_data.first())
    val user_fields = user_data.map(l => l.split("\\|"))

    val num_users = user_fields.map(l => l(0)).count()
    //val median_rating = math.median(ratings.collect()) function not supported - TODO
    val ratings_per_user = num_ratings / num_users
    println("ratings per user: " + ratings_per_user)

    val num_movies = Util.getMovieData().count()
    val ratings_per_movie = num_ratings / num_movies
    println("ratings per movie: " + ratings_per_movie)
    val count_by_rating = ratings.countByValue()
    println("count_by_rating: + " + count_by_rating)

    println(ratings.stats())

    val user_ratings_grouped = rating_data.map(fields => (fields(0).toInt, fields(2).toInt)).groupByKey()
    //Python code : user_ratings_byuser = user_ratings_grouped.map(lambda (k, v): (k, len(v)))
    val user_ratings_byuser = user_ratings_grouped.map(v =>  (v._1,v._2.size))
    val user_ratings_byuser_take5 = user_ratings_byuser.take(5)
    user_ratings_byuser_take5.foreach(println)
    val user_ratings_byuser_local = user_ratings_byuser.map(v =>  v._2).collect()
    val movie_fields = Util.movieFields()

    //TODO - Change the python code below
    //idx_bad_data = np.where(years_pre_processed_array==1900)[0][0]
    //years_pre_processed_array[idx_bad_data] = median_year

    //Feature Extraction
    //Categorical Features: _1-of-k_ Encoding of User Occupation

    val all_occupations = user_fields.map(fields=> fields(3)).distinct().collect()
    scala.util.Sorting.quickSort(all_occupations)

    var all_occupations_dict:Map[String, Int] = Map()
    var idx = 0;
    // for loop execution with a range
    for( idx <- 0 to (all_occupations.length -1)){
      all_occupations_dict += all_occupations(idx) -> idx
    }

    println("Encoding of 'doctor : " + all_occupations_dict("doctor"))
    println("Encoding of 'programmer' : " + all_occupations_dict("programmer"))
    /*
    K = len(all_occupations_dict)
    binary_x = np.zeros(K)
    k_programmer = all_occupations_dict['programmer']
    binary_x[k_programmer] = 1
    print "Binary feature vector: %s" % binary_x
    print "Length of binary vector: %d" % K
     */
    val k = all_occupations_dict.size
    val binary_x = DenseVector.zeros[Double](k)
    val k_programmer = all_occupations_dict("programmer")
    binary_x(k_programmer) = 1
    println("Binary feature vector: %s" + binary_x)
    println("Length of binary vector: " + k)

    val timestamps = rating_data.map(fields=> fields(3))
    val hour_of_day = timestamps.map(ts => getCurrentHour(ts))
    println(hour_of_day.take(5).foreach(println))

    //time_of_day = hour_of_day.map(lambda hr: assign_tod(hr))
    val time_of_day = hour_of_day.map(hr => assignTod(hr))
    println(time_of_day.take(5).foreach(println))

  }


  def getCurrentHour(dateStr: String) : Integer = {
    var currentHour = 0
    try {
      val date = new Date(dateStr.toLong)
      return int2Integer(date.getHours)
    } catch {
      case _ => return currentHour
    }
    return 1
  }

  /*
  def assign_tod(hr):
    times_of_day = {
                'morning' : range(7, 12),
                'lunch' : range(12, 14),
                'afternoon' : range(14, 18),
                'evening' : range(18, 23),
                'night' : range(23, 7)
                }
    for k, v in times_of_day.iteritems():
        if hr in v:
            return k
   */

  def assignTod(hr : Integer) : String = {
    if(hr >= 7 && hr < 12){
      return "morning"
    }else if ( hr >= 12 && hr < 14) {
      return "lunch"
    } else if ( hr >= 14 && hr < 18) {
      return "afternoon"
    } else if ( hr >= 18 && hr.<(23)) {
      return "evening"
    } else if ( hr >= 23 && hr <= 24) {
      return "night"
    } else if (  hr < 7) {
      return "night"
    } else {
      return "error"
    }
  }

  def mean( x:Array[Int]) : Int = {
    return x.sum/x.length
  }

  def median( x:Array[Int]) : Int = {
    val middle = x.length/2
    if (x.length%2 == 1) {
      return x(middle)
    } else {
      return (x(middle-1) + x(middle)) / 2
    }
  }
}