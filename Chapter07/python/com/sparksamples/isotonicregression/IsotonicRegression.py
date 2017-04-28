import sys
import os
from pyspark.mllib.regression import LabeledPoint

from pyspark.mllib.regression import IsotonicRegression, IsotonicRegressionModel

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_sum_feature
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
    for m in mappings:
        print m
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len
    #data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))

    parsed_data = records.map(lambda r : (extract_label(r),extract_sum_feature(r,cat_len,mappings), 1.0 ))
    model = IsotonicRegression.train(parsed_data)

    first = parsed_data.first()
    print first

    true_vs_predicted = parsed_data.map(lambda p: (p[1], model.predict(p[2])))
    print "Isotonic Regression: " + str(true_vs_predicted.take(5))

    calculate_print_metrics("Isotonic Regression", true_vs_predicted)


if __name__ == "__main__":
    main()

