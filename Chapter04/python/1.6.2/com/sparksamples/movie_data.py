from util import get_movie_data
import matplotlib.pyplot as plt
import matplotlib

def main():
    movie_data = get_movie_data()
    print movie_data.first()
    num_movies = movie_data.count()
    print "Movies: %d" % num_movies
    movie_fields = movie_data.map(lambda lines: lines.split("|"))
    years = movie_fields.map(lambda fields: fields[2]).map(lambda x: convert_year(x))
    # we filter out any 'bad' data points here
    years_filtered = years.filter(lambda x: x != 1900)
    # plot the movie ages histogram
    movie_ages = years_filtered.map(lambda yr: 1998-yr).countByValue()
    values = movie_ages.values()
    bins = movie_ages.keys()
    plt.hist(values, bins=bins, color='lightblue', normed=True)
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(16,10)
    plt.show()


def convert_year(x):
    try:
        return int(x[-4:])
    except:
        # there is a 'bad' data point with a blank year, which we set to 1900 and will filter out later
        return 1900


if __name__ == "__main__":
    main()