import os
import sys

import pylab as P
import matplotlib
import matplotlib.pyplot as plt

from com.sparksamples.util import evaluate
from com.sparksamples.linearregression.LinearRegressionUtil import get_train_test_data


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
    params = [1, 5, 10, 20, 50, 100, 200]
    metrics = [evaluate(train_data, test_data, param, 0.01, 0.0, 'l2', False) for param in params]
    print params
    print metrics
    P.plot(params, metrics)
    fig = matplotlib.pyplot.gcf()
    plt.xscale('log')
    plt.title("LinearRegressionWithSGD : Iterations")
    plt.xlabel("Iterators")
    plt.ylabel("RMSLE")
    P.show()


if __name__ == "__main__":
    main()

