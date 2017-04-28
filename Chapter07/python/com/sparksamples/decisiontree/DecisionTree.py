import os
import sys

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import extract_features_dt
from com.sparksamples.util import get_records
from com.sparksamples.util import calculate_print_metrics
from com.sparksamples.util import SPARK_HOME

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
    records.cache()

    # extract all the catgorical mappings
    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len
    print "Feature vector length for categorical features: %d" % cat_len
    print "Feature vector length for numerical features: %d" % num_len
    print "Total feature vector length: %d" % total_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))

    data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))
    first_point_dt = data_dt.first()
    print "Decision Tree feature vector: " + str(first_point_dt.features)
    print "Decision Tree feature vector length: " + str(len(first_point_dt.features))

    dt_model = DecisionTree.trainRegressor(data_dt, {})
    preds = dt_model.predict(data_dt.map(lambda p: p.features))
    actual = data.map(lambda p: p.label)
    true_vs_predicted_dt = actual.zip(preds)
    print "Decision Tree predictions: " + str(true_vs_predicted_dt.take(5))
    print "Decision Tree depth: " + str(dt_model.depth())
    print "Decision Tree number of nodes: " + str(dt_model.numNodes())

    calculate_print_metrics("Decision Tree", true_vs_predicted_dt)


if __name__ == "__main__":
    main()

