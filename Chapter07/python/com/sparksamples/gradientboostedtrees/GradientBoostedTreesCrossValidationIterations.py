import os
import sys

import pylab as P
import matplotlib
import matplotlib.pyplot as plt

from com.sparksamples.gradientboostedtrees.GradientBoostedTreesUtil import get_train_test_data
from com.sparksamples.gradientboostedtrees.GradientBoostedTreesUtil import evaluate_gbt

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

from com.sparksamples.util import SPARK_HOME

os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME + "/python")


def main():
    execute()


def execute():
    train_data, test_data = get_train_test_data()
    params = [1, 5, 10, 20]
    lrRate = 0.1
    mxDepth = 3
    mxBins = 32
    #def evaluate_gbt(train, test, numItr, lrRate, mxDepth, mxBins):
    # def trainRegressor(cls, data, categoricalFeaturesInfo,
    #                   loss="leastSquaresError", numIterations=100, learningRate=0.1, maxDepth=3,
    #                   maxBins=32):
    metrics = [evaluate_gbt(train_data, test_data, param, lrRate, mxDepth, mxBins ) for param in params]
    print params
    print metrics
    P.plot(params, metrics)
    fig = matplotlib.pyplot.gcf()
    plt.xscale('log')
    plt.title('GradientBoosted Trees - Iterators')
    plt.xlabel('iterations - log scale')
    plt.ylabel('RMSLE')

    P.show()


if __name__ == "__main__":
    main()

