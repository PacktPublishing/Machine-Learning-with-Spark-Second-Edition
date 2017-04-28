import numpy as np

from com.sparksamples.util import get_records
from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import extract_features_dt

from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.regression import LabeledPoint
from com.sparksamples.util import squared_log_error
__author__ = 'Rajdeep Dua'


def evaluate_dt(train, test, maxDepth, maxBins):
    model = DecisionTree.trainRegressor(train, {}, impurity='variance', maxDepth=maxDepth, maxBins=maxBins)
    preds = model.predict(test.map(lambda p: p.features))
    actual = test.map(lambda p: p.label)
    tp = actual.zip(preds)
    rms_le = np.sqrt(tp.map(lambda (t, p): squared_log_error(t, p)).mean())
    return rms_le


def get_train_test_data():
    records = get_records()
    records.cache()

    # extract all the catgorical mappings
    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))

    data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))
    data_with_idx_dt = data_dt.zipWithIndex().map(lambda (k, v): (v, k))
    test_dt = data_with_idx_dt.sample(False, 0.2, 42)
    train_dt = data_with_idx_dt.subtractByKey(test_dt)

    train_data_dt = train_dt.map(lambda (idx, p): p)
    test_data_dt = test_dt.map(lambda (idx, p) : p)
    return train_data_dt, test_data_dt