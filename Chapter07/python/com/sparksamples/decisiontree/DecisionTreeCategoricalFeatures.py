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

    print "Mapping of first categorical feature column: %s" % get_mapping(records, 2)

    # extract all the catgorical mappings
    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))

    data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))
    cat_features = dict([(i - 2, len(get_mapping(records, i)) + 1) for i in range(2,10)])
    print "Categorical feature size mapping %s" % cat_features
    # train the model again
    dt_model = DecisionTree.trainRegressor(data_dt, categoricalFeaturesInfo=cat_features)
    preds = dt_model.predict(data_dt.map(lambda p: p.features))
    actual = data.map(lambda p: p.label)
    true_vs_predicted_dt = actual.zip(preds)

    calculate_print_metrics("Decision Tree Categorical Features", true_vs_predicted_dt)


if __name__ == "__main__":
    main()

