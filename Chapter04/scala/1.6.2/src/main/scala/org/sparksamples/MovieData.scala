package org.sparksamples

/**
  * Created by Rajdeep on 12/22/15.
  */
import scala.collection.immutable.ListMap

object MovieData {

  def main(args: Array[String]) {
    val movie_data = Util.getMovieData()
    println(movie_data.first())

    val num_movies = movie_data.count()
    println("num_movies: " + num_movies)

    val movie_fields = movie_data.map(lines =>  lines.split("\\|"))
    val years = movie_fields.map( field => field(2)).map( x => Util.convertYear(x))
    val years_filtered = years.filter(x => (x != 1900) )
    val movie_ages = years_filtered.map(yr =>  (1998-yr) ).countByValue()
    val movie_ages_sorted = ListMap(movie_ages.toSeq.sortBy(_._1):_*)

    val values = movie_ages.values
    val bins = movie_ages.keys

    println("movie_fields: " + movie_fields)
    println("years: " + years)
    println("years_filtered: " + years_filtered)
    println("movie_ages: " + movie_ages_sorted)
    println("values: " + values)
    println("bins: " + bins)
  }


}