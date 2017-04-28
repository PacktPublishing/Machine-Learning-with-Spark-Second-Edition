import os
import sys

import pylab as P
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import bar

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
    xticks = ["False", "True"]
    params = [False, True]
    x = [0,1]
    metrics = [evaluate(train_data, test_data, 10, 0.1, 1.0, 'l2', param) for param in params]
    #metrics = [1.4064367998474196, 1.4358305401660159]
    print params
    print metrics
    P.plot(x, metrics)
    P.xticks(params,xticks)
    plt.title("LinearRegressionWithSGD : Intercept")
    plt.xlabel("Intercept Boolean")
    plt.ylabel("RMSLE")

    P.show()


if __name__ == "__main__":
    main()

