import sys
import numpy as np

from pyspark.mllib.regression import LinearRegressionWithSGD
try:
    from pyspark import SparkContext
    from pyspark import SparkConf
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)
PROJECT_HOME = "/home/ubuntu/work/ml-resources/spark-ml"
path = PROJECT_HOME + "/Chapter_07/data/hour_noheader.csv"
SPARK_HOME = "/home/ubuntu/work/spark-1.6.1-bin-hadoop2.6/"
def get_records():
    conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         )
    sc = SparkContext(conf =conf)
    raw_data = sc.textFile(path)
    num_data = raw_data.count()
    records = raw_data.map(lambda x: x.split(","))
    return records

def calculate_print_metrics(model_name, true_vs_predicted):
    mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
    mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean()
    rmsle = np.sqrt(true_vs_predicted.map(lambda (t, p): squared_log_error(t, p)).mean())
    print model_name + " - Mean Squared Error: %2.4f" % mse
    print model_name + " - Mean Absolute Error: %2.4f" % mae
    print model_name + " - Root Mean Squared Log Error: %2.4f" % rmsle

def get_mapping(rdd, idx):
    x = rdd.map(lambda fields: fields[idx]).distinct()

    print "x.zipWithIndex(): " + str(x.zipWithIndex())
    return x.zipWithIndex().collectAsMap()

def extract_features(record,cat_len, mappings):
    cat_vec = np.zeros(cat_len)
    i = 0
    step = 0
    for field in record[2:9]:
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i = i + 1
        step = step + len(m)

    num_vec = np.array([float(field) for field in record[10:14]])
    return np.concatenate((cat_vec, num_vec))

def extract_label(record):
    print record[-1]
    return float(record[-1])

def extract_features_dt(record):
    x = np.array(map(float, record[2:14]))
    print str(x)
    return np.array(map(float, record[2:14]))

def extract_sum_feature(record,cat_len, mappings):
    x = extract_features(record, cat_len, mappings)
    sum = 0.0
    length = len(record)
    for index in range(length):
        sum += x[index]
        print sum

    return sum
# def extractSumFeature(record : Array[String], cat_len: Int,
#                       mappings:scala.collection.immutable.List[scala.collection.Map[String,Long]]): Double ={
#     //var cat_vec = Vectors.zeros(cat_len)
#     //val cat_array = cat_vec.toArray
#     val x = extractFeatures(record, cat_len, mappings)
#     var sum = 0.0
#     for(y <- 0 until 14){
#
#         sum = sum + x(y).toDouble
#     }
#     return sum.toDouble
#   }

def squared_error(actual, pred):
    return (pred - actual)**2

def abs_error(actual, pred):
    return np.abs(pred - actual)

def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1))**2

def evaluate(train, test, iterations, step, regParam, regType, intercept):
    model = LinearRegressionWithSGD.train(train, iterations, step, regParam=regParam, regType=regType, intercept=intercept)
    tp = test.map(lambda p: (p.label, model.predict(p.features)))
    rmsle = np.sqrt(tp.map(lambda (t, p): squared_log_error(t, p)).mean())
    return rmsle