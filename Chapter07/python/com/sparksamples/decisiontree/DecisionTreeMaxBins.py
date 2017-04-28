import os
import sys

import pylab as P
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import bar

from com.sparksamples.decisiontree.DecisionTreeUtil import evaluate_dt
from com.sparksamples.decisiontree.DecisionTreeUtil import get_train_test_data


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
    train_data_dt, test_data_dt = get_train_test_data()
    params = [2, 4, 8, 16, 32, 64, 100]
    metrics = [evaluate_dt(train_data_dt, test_data_dt, 5, param) for param in params]
    print params
    print metrics
    P.plot(params, metrics)
    fig = matplotlib.pyplot.gcf()
    plt.title('Decision Trees - Max Bins')
    plt.xlabel('Max Bins')
    plt.ylabel('RMSLE')

    P.show()


if __name__ == "__main__":
    main()

