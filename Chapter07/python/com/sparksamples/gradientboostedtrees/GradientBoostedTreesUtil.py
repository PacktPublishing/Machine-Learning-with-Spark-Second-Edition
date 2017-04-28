import numpy as np

from com.sparksamples.util import get_records
from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import extract_features_dt

#from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.regression import LabeledPoint
from com.sparksamples.util import squared_log_error
__author__ = 'Rajdeep Dua'


def evaluate_gbt(train, test, numItr, lrRate, mxDepth, mxBins):
     # def trainRegressor(cls, data, categoricalFeaturesInfo,
     #                   loss="leastSquaresError", numIterations=100, learningRate=0.1, maxDepth=3,
     #                   maxBins=32):
    gbt_model = GradientBoostedTrees.trainRegressor(train,categoricalFeaturesInfo={}, numIterations=numItr,
                                                    maxDepth=mxDepth, maxBins=mxBins, learningRate=lrRate)
    predictions = gbt_model.predict(test.map(lambda x: x.features))
    tp = test.map(lambda lp: lp.label).zip(predictions)
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

    #data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))
    data_with_idx = data.zipWithIndex().map(lambda (k, v): (v, k))
    test = data_with_idx.sample(False, 0.2, 42)
    train = data_with_idx.subtractByKey(test)

    train_data = train.map(lambda (idx, p): p)
    test_data = test.map(lambda (idx, p) : p)
    return train_data, test_data
