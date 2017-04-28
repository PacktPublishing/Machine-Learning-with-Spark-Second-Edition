package org.sparksamples
/**
  * Created by Rajdeep on 12/22/15.
  */

object MovieData {

    def getMovieYearsCountSorted(): scala.Array[(Int,String)] = {
    val movie_data_df = Util.getMovieDataDF()
    movie_data_df.createOrReplaceTempView("movie_data")
    movie_data_df.printSchema()

    Util.spark.udf.register("convertYear", Util.convertYear _)
    movie_data_df.show(false)

    val movie_years = Util.spark.sql("select convertYear(date) as year from movie_data")
    val movie_years_count = movie_years.groupBy("year").count()
    movie_years_count.show(false)
    val movie_years_count_rdd = movie_years_count.rdd.map(row => (Integer.parseInt(row(0).toString), row(1).toString))
    val movie_years_count_collect = movie_years_count_rdd.collect()
    val movie_years_count_collect_sort = movie_years_count_collect.sortBy(_._1)
    return movie_years_count_collect_sort

  }
  def main(args: Array[String]) {

    val movie_years = MovieData.getMovieYearsCountSorted()
    for( a <- 0 to (movie_years.length -1)){
      println(movie_years(a))
    }

  }



}