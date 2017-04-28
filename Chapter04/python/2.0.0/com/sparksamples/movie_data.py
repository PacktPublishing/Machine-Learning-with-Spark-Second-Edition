from util import get_movie_data
from util import spark
import matplotlib.pyplot as plt
import matplotlib


def main():
    movie_data = get_movie_data()

    print movie_data.first()
    num_movies = movie_data.count()
    print "Movies: %d" % num_movies
    #movie_years = movie_data.select("year")
    #from pyspark.sql.functions import udf
    #from pyspark.sql import SparkSession

    spark.udf.register("convert_year", convert_year)
    # Bug in pyspark 2.0.0 reverting to RDD
    # https://issues.apache.org/jira/browse/SPARK-17538
    #movie_data.createTempView("movie_data")
    #movie_years = spark.sql("select convertYear(date) as year from movie_data")
    #print(movie_years.first)

    movie_fields = movie_data.map(lambda lines: lines.split("|"))
    print(len(movie_fields.first()))
    years = movie_fields.map(lambda fields: fields[2]).map(lambda x: convert_year(x))
    # # we filter out any 'bad' data points here
    years_filtered = years.filter(lambda x: x != 1900)
    # plot the movie ages histogram
    movie_ages = years_filtered.map(lambda yr: 1998-yr).countByValue()
    values = movie_ages.values()
    bins = movie_ages.keys()
    plt.hist(values, bins=bins, color='lightblue', normed=True)
    plt.xticks(fontsize='12')

    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(16, 10)

    for tick in f.xaxis.get_major_ticks():
        tick.label.set_fontsize(8)
                # specify integer or one of preset strings, e.g.
                #tick.label.set_fontsize('x-small')
        tick.label.set_rotation('vertical')
    plt.show()


def convert_year(x):
    try:
        return int(x[-4:])
    except:
        # there is a 'bad' data point with a blank year, which we set to 1900 and will filter out later
        return 1900


if __name__ == "__main__":
    main()