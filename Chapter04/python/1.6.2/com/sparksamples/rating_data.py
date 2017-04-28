from util import get_rating_data
from util import get_user_data
from util import get_movie_data
import numpy as np


def main():
    rating_data_raw = get_rating_data()
    print rating_data_raw.first()
    num_ratings = rating_data_raw.count()
    print "Ratings: %d" % num_ratings
    num_movies = get_movie_data().count()
    num_users = get_user_data().count()

    rating_data = rating_data_raw.map(lambda line: line.split("\t"))
    ratings = rating_data.map(lambda fields: int(fields[2]))
    max_rating = ratings.reduce(lambda x, y: max(x, y))
    min_rating = ratings.reduce(lambda x, y: min(x, y))
    mean_rating = ratings.reduce(lambda x, y: x + y) / float(num_ratings)
    median_rating = np.median(ratings.collect())
    ratings_per_user = num_ratings / num_users
    ratings_per_movie = num_ratings / num_movies
    print "Min rating: %d" % min_rating
    print "Max rating: %d" % max_rating
    print "Average rating: %2.2f" % mean_rating
    print "Median rating: %d" % median_rating
    print "Average # of ratings per user: %2.2f" % ratings_per_user
    print "Average # of ratings per movie: %2.2f" % ratings_per_movie


if __name__ == "__main__":
    main()