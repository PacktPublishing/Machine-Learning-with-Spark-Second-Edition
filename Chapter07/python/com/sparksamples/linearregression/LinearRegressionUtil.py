import os
import sys

from pyspark.mllib.regression import LabeledPoint

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import get_records


try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

from com.sparksamples.util import SPARK_HOME

os.environ['SPARK_HOME'] = SPARK_HOME
sys.path.append(SPARK_HOME + "/python")

def get_train_test_data():
    records = get_records()
    records.cache()
    print "Mapping of first categorical feature column: %s" % get_mapping(records, 2)

    mappings = [get_mapping(records, i) for i in range(2,10)]
    for m in mappings:
        print m
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))
    data_with_idx = data.zipWithIndex().map(lambda (k, v): (v, k))
    test = data_with_idx.sample(False, 0.2, 42)
    train = data_with_idx.subtractByKey(test)

    train_data = train.map(lambda (idx, p): p)
    test_data = test.map(lambda (idx, p) : p)

    train_size = train_data.count()
    test_size = test_data.count()
    num_data = data.count()
    print "Training data size: %d" % train_size
    print "Test data size: %d" % test_size
    print "Total data size: %d " % num_data
    print "Train + Test size : %d" % (train_size + test_size)

    return train_data, test_data


