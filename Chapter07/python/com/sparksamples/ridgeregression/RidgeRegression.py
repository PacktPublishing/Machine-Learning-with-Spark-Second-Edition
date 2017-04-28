import os
import sys

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import RidgeRegressionWithSGD

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import get_records
from com.sparksamples.util import calculate_print_metrics
from com.sparksamples.util import SPARK_HOME
os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME + "/python")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

def main():
    records = get_records()
    records.cache()

    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))

    rr_model = RidgeRegressionWithSGD.train(data, iterations=10, step=0.1, intercept=False)
    true_vs_predicted_rr = data.map(lambda p: (p.label, rr_model.predict(p.features)))

    print "Ridge Regression Model predictions: " + str(true_vs_predicted_rr.take(5))

    calculate_print_metrics("Ridge Regression", true_vs_predicted_rr)


if __name__ == "__main__":
    main()

