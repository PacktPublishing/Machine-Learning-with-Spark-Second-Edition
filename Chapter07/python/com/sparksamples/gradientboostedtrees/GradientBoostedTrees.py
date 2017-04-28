import os
import sys
import numpy as np

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees

from com.sparksamples.util import get_mapping
from com.sparksamples.util import extract_features
from com.sparksamples.util import extract_label
from com.sparksamples.util import get_records

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
    first = records.first()
    records.cache()

    # extract all the catgorical mappings
    mappings = [get_mapping(records, i) for i in range(2,10)]
    cat_len = sum(map(len, mappings))
    num_len = len(records.first()[11:15])
    total_len = num_len + cat_len

    data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, cat_len, mappings)))
    first_point = data.first()

    gbt_model = GradientBoostedTrees.trainRegressor(data,categoricalFeaturesInfo={}, numIterations=3)
    true_vs_predicted_gbt = data.map(lambda p: (p.label, gbt_model.predict(p.features)))

    predictions = gbt_model.predict(data.map(lambda x: x.features))
    labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
    print "GradientBoosted Trees predictions: " + str(labelsAndPredictions.take(5))

    mse = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() /\
        float(data.count())
    mae = labelsAndPredictions.map(lambda (v, p): np.abs(v - p)).sum() /\
        float(data.count())
    rmsle = labelsAndPredictions.map(lambda (v,p) :  ((np.log(p + 1) - np.log(v + 1))**2)).sum() /\
        float(data.count())
    print('Gradient Boosted Trees - Mean Squared Error = ' + str(mse))
    print('Gradient Boosted Trees - Mean Absolute Error = ' + str(mae))
    print('Gradient Boosted Trees - Mean Root Mean Squared Log Error = ' + str(rmsle))


if __name__ == "__main__":
    main()

