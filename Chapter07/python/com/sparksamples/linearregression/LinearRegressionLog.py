import sys
import numpy as np
import os
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import path
from com.sparksamples.util import calculate_print_metrics
from com.sparksamples.util import SPARK_HOME
from com.sparksamples.util import get_records

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME + "/python")


def main():
    records = get_records()
    mappings = [get_mapping(records, i) for i in range(2,10)]

    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))
    data_log = data.map(lambda lp: LabeledPoint(np.log(lp.label), lp.features))
    model_log = LinearRegressionWithSGD.train(data_log, iterations=10, step=0.1)
    true_vs_predicted_log = data_log.map(lambda p: (np.exp(p.label), np.exp(model_log.predict(p.features))))
    calculate_print_metrics("Linear Regression Log", true_vs_predicted_log)


if __name__ == "__main__":
    main()

