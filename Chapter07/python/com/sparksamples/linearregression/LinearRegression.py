import sys
import os
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import get_records
from com.sparksamples.util import SPARK_HOME
from com.sparksamples.util import calculate_print_metrics

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
    print records.first()
    print records.count()
    records.cache()
    print "Mapping of first categorical feature column: %s" % get_mapping(records, 2)

    mappings = [get_mapping(records, i) for i in range(2,10)]
    for m in mappings:
        print m
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len
    print "Feature vector length for categorical features: %d" % cat_len
    print "Feature vector length for numerical features: %d" % num_len
    print "Total feature vector length: %d" % total_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))
    first_point = data.first()

    print "Linear Model feature vector:\n" + str(first_point.features)
    print "Linear Model feature vector length: " + str(len(first_point.features))

    #linear_model = LinearRegressionWithSGD.train(data, iterations=10, step=0.1, intercept=False)
    linear_model = LinearRegressionWithSGD.train(data, iterations=10, step=0.025, regParam=0.0, regType=None,
                                  intercept=False)

    true_vs_predicted = data.map(lambda p: (p.label, linear_model.predict(p.features)))
    print "Linear Model predictions: " + str(true_vs_predicted.take(5))

    calculate_print_metrics("Linear Regression", true_vs_predicted)


if __name__ == "__main__":
    main()

