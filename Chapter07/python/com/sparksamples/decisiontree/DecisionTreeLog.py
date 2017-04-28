import os
import sys
import numpy as np

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

    # extract all the catgorical mappings
    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))
    data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))


    dt_model = DecisionTree.trainRegressor(data_dt, {})
    preds = dt_model.predict(data_dt.map(lambda p: p.features))
    actual = data.map(lambda p: p.label)
    true_vs_predicted_dt = actual.zip(preds)

    data_dt_log = data_dt.map(lambda lp: LabeledPoint(np.log(lp.label), lp.features))
    dt_model_log = DecisionTree.trainRegressor(data_dt_log, {})

    preds_log = dt_model_log.predict(data_dt_log.map(lambda p: p.features))
    actual_log = data_dt_log.map(lambda p: p.label)
    true_vs_predicted_dt_log = actual_log.zip(preds_log).map(lambda (t, p): (np.exp(t), np.exp(p)))

    calculate_print_metrics("Decision Tree Log", true_vs_predicted_dt_log)


if __name__ == "__main__":
    main()

