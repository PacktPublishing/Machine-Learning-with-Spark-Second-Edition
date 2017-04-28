package org.sparksamples.classification

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.optimization.{SimpleUpdater, SquaredL2Updater, Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Impurity}
import org.apache.spark.rdd.RDD
import org.sparksamples.classification.stumbleupon.SparkConstants

/**
	* @author manpreet.singh
	*/

object AllInOneClassification {

	def main(args: Array[String]) {
		val sc = new SparkContext("local[1]", "Classification")

		// get StumbleUpon dataset 'https://www.kaggle.com/c/stumbleupon'
		val records = sc.textFile(SparkConstants.PATH + "data/train_noheader.tsv").map(line => line.split("\t"))
			//.map(records => (records(0), records(1), records(2)))

		//records.foreach(println)

		// for linear models
		val data = records.map { r =>
			val trimmed = r.map(_.replaceAll("\"", ""))
			val label = trimmed(r.size - 1).toInt
			val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
			LabeledPoint(label, Vectors.dense(features))
		}


    val data_persistent = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      val len = features.size.toInt
      val len_2 = math.floor(len/2).toInt
      val x = features.slice(0,len_2)

      val y = features.slice(len_2 -1 ,len )
      var i=0
      var sum_x = 0.0
      var sum_y = 0.0
      while (i < x.length) {
        sum_x += x(i)
        i += 1
      }
      i = 0
      while (i < y.length) {
        sum_y += y(i)
        i += 1
      }
      if (sum_y != 0.0) {
        if(sum_x != 0.0) {
          math.log(sum_x) + "," + math.log(sum_y)
        }else {
          sum_x + "," + math.log(sum_y)
        }

      }else {
        if(sum_x != 0.0) {
          math.log(sum_x) + "," + 0.0
        }else {
          sum_x + "," + 0.0
        }
      }

    }
    val dataone = data_persistent.first()

    data_persistent.saveAsTextFile(SparkConstants.PATH + "/results/raw-input-log")

		// for nb model
		val nbData = records.map { r =>
			val trimmed = r.map(_.replaceAll("\"", ""))
			val label = trimmed(r.size - 1).toInt
			val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble).map(d => if (d < 0) 0.0 else d)
			LabeledPoint(label, Vectors.dense(features))
		}

		data.cache()

		val numData = data.count()
		println(numData)

    // params for logistic regression and SVM
		val numIterations = 10
		// params for decision tree
		val maxTreeDepth = 5

		val lrModel = LogisticRegressionWithSGD.train(data, numIterations)
		val svmModel = SVMWithSGD.train(data, numIterations)

		val nbModel = NaiveBayes.train(nbData)

		val dtModel = DecisionTree.train(data, Algo.Classification, Entropy, maxTreeDepth)

		// single datapoint prediction
		val dataPoint = data.first()
		val prediction = lrModel.predict(dataPoint.features)
		println(prediction)
		println(dataPoint.label)

		// bulk prediction
		val predictions = lrModel.predict(data.map(lp => lp.features))
		predictions.foreach(println)

		// accuracy and prediction error
		val lrTotalCorrect = data.map { point =>
			if(lrModel.predict(point.features) == point.label) 1 else 0
		}.sum()

		val svmTotalCorrect = data.map { point =>
			if(svmModel.predict(point.features) == point.label) 1 else 0
		}.sum()

		val nbTotalCorrect = data.map { point =>
			if(nbModel.predict(point.features) == point.label) 1 else 0
		}.sum()

		val dtTotalCorrect = data.map { point =>
			val score = dtModel.predict(point.features)
			val predicted = if (score > 0.5) 1 else 0
			if(predicted == point.label) 1 else 0
		}.sum()

		val lrAccuracy = lrTotalCorrect / data.count()
		println(lrAccuracy)

		val svmAccuracy = svmTotalCorrect / data.count()
		println(svmAccuracy)

		val nbAccuracy = nbTotalCorrect / data.count()
		println(nbAccuracy)

		val dtAccuracy = dtTotalCorrect / data.count()
		println(dtAccuracy)

    // ROC curve and AUC
		val metrics = Seq(lrModel, svmModel).map { model =>
			val scoreAndLabels = data.map { point =>
				(model.predict(point.features), point.label)
			}
			val metrics = new BinaryClassificationMetrics(scoreAndLabels)
			(model.getClass.getSimpleName, metrics.areaUnderPR, metrics.
				areaUnderROC)
		}

		val nbMetrics = Seq(nbModel).map{ model =>
			val scoreAndLabels = nbData.map { point =>
				val score = model.predict(point.features)
				(if (score > 0.5) 1.0 else 0.0, point.label)
			}
			val metrics = new BinaryClassificationMetrics(scoreAndLabels)
			(model.getClass.getSimpleName, metrics.areaUnderPR,
				metrics.areaUnderROC)
		}

		val dtMetrics = Seq(dtModel).map{ model =>
			val scoreAndLabels = data.map { point =>
				val score = model.predict(point.features)
				(if (score > 0.5) 1.0 else 0.0, point.label)
			}
			val metrics = new BinaryClassificationMetrics(scoreAndLabels)
			(model.getClass.getSimpleName, metrics.areaUnderPR,
				metrics.areaUnderROC)
		}

		val allMetrics = metrics ++ nbMetrics ++ dtMetrics
		allMetrics.foreach{ case (m, pr, roc) =>
			println(f"$m, Area under PR: ${pr * 100.0}%2.4f%%, Area under ROC: ${roc * 100.0}%2.4f%%")
		}

    // feature standardization
		val vectors = data.map(lp => lp.features)
		val matrix = new RowMatrix(vectors)
		val matrixSummary = matrix.computeColumnSummaryStatistics()
		println(matrixSummary.mean)
		println(matrixSummary.min)
		println(matrixSummary.max)
		println(matrixSummary.variance)
		println(matrixSummary.numNonzeros)

		val scaler = new StandardScaler(withMean = true, withStd =
			true).fit(vectors)
		val scaledData = data.map(lp => LabeledPoint(lp.label,
			scaler.transform(lp.features)))
		println(data.first.features)
		println(scaledData.first.features)
		println((0.789131 - 0.41225805299526636)/ math.sqrt(0.1097424416755897))

		val lrModelScaled = LogisticRegressionWithSGD.train(scaledData, numIterations)
		val lrTotalCorrectScaled = scaledData.map { point =>
			if (lrModelScaled.predict(point.features) == point.label) 1 else
				0 }.sum
		val lrAccuracyScaled = lrTotalCorrectScaled / numData
		val lrPredictionsVsTrue = scaledData.map { point =>
			(lrModelScaled.predict(point.features), point.label)
		}
		val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
		val lrPr = lrMetricsScaled.areaUnderPR
		val lrRoc = lrMetricsScaled.areaUnderROC
		println(f"${lrModelScaled.getClass.getSimpleName}\nAccuracy: ${lrAccuracyScaled * 100}%2.4f%%\nArea under PR: ${lrPr * 100.0}%2.4f%%\nArea under ROC: ${lrRoc * 100.0}%2.4f%%")

		// additional features
		val categories = records.map(r => r(3)).distinct.collect.zipWithIndex.toMap
		val numCategories = categories.size
		println(categories)
		println(numCategories)

		val dataCategories = records.map { r =>
			val trimmed = r.map(_.replaceAll("\"", ""))
			val label = trimmed(r.size - 1).toInt
			val categoryIdx = categories(r(3))
			val categoryFeatures = Array.ofDim[Double](numCategories)
			categoryFeatures(categoryIdx) = 1.0
			val otherFeatures = trimmed.slice(4, r.size - 1).map(d => if
			(d == "?") 0.0 else d.toDouble)
			val features = categoryFeatures ++ otherFeatures
			LabeledPoint(label, Vectors.dense(features))
		}
		println(dataCategories.first)

		val scalerCats = new StandardScaler(withMean = true, withStd = true).fit(dataCategories.map(lp => lp.features))
		val scaledDataCats = dataCategories.map(lp => LabeledPoint(lp.label, scalerCats.transform(lp.features)))
		println(dataCategories.first.features)
		println(scaledDataCats.first.features)

		val lrModelScaledCats = LogisticRegressionWithSGD.
			train(scaledDataCats, numIterations)
		val lrTotalCorrectScaledCats = scaledDataCats.map { point =>
			if (lrModelScaledCats.predict(point.features) == point.label) 1 else
				0
		}.sum
		val lrAccuracyScaledCats = lrTotalCorrectScaledCats / numData
		val lrPredictionsVsTrueCats = scaledDataCats.map { point =>
			(lrModelScaledCats.predict(point.features), point.label)
		}
		val lrMetricsScaledCats = new BinaryClassificationMetrics(lrPredictionsVsTrueCats)
		val lrPrCats = lrMetricsScaledCats.areaUnderPR
		val lrRocCats = lrMetricsScaledCats.areaUnderROC
		println(f"${lrModelScaledCats.getClass.getSimpleName}\nAccuracy: ${lrAccuracyScaledCats * 100}%2.4f%%\nArea under PR: ${lrPrCats * 100.0}%2.4f%%\nArea under ROC: ${lrRocCats * 100.0}%2.4f%%")

		// using the correct form of data
		val dataNB = records.map { r =>
			val trimmed = r.map(_.replaceAll("\"", ""))
			val label = trimmed(r.size - 1).toInt
			val categoryIdx = categories(r(3))
			val categoryFeatures = Array.ofDim[Double](numCategories)
			categoryFeatures(categoryIdx) = 1.0
			LabeledPoint(label, Vectors.dense(categoryFeatures))
		}

		val nbModelCats = NaiveBayes.train(dataNB)
		val nbTotalCorrectCats = dataNB.map { point =>
			if (nbModelCats.predict(point.features) == point.label) 1 else 0
		}.sum
		val nbAccuracyCats = nbTotalCorrectCats / numData
		val nbPredictionsVsTrueCats = dataNB.map { point =>
			(nbModelCats.predict(point.features), point.label)
		}
		val nbMetricsCats = new BinaryClassificationMetrics(nbPredictionsVsTrueCats)
		val nbPrCats = nbMetricsCats.areaUnderPR
		val nbRocCats = nbMetricsCats.areaUnderROC
		println(f"${nbModelCats.getClass.getSimpleName}\nAccuracy: ${nbAccuracyCats * 100}%2.4f%%\nArea under PR: ${nbPrCats * 100.0}%2.4f%%\nArea under ROC: ${nbRocCats * 100.0}%2.4f%%")

		// investigate the impact of model parameters on performance
		// helper function to train a logistic regresson model
		def trainWithParams(input: RDD[LabeledPoint], regParam: Double, numIterations: Int, updater: Updater, stepSize: Double) = {
			val lr = new LogisticRegressionWithSGD
			lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
			lr.run(input)
		}
		// helper function to create AUC metric
		def createMetrics(label: String, data: RDD[LabeledPoint], model: ClassificationModel) = {
			val scoreAndLabels = data.map { point =>
				(model.predict(point.features), point.label)
			}
			val metrics = new BinaryClassificationMetrics(scoreAndLabels)
			(label, metrics.areaUnderROC)
		}

		// cache the data to increase speed of multiple runs agains the dataset
		scaledDataCats.cache
		// num iterations
		val iterResults = Seq(1, 5, 10, 50).map { param =>
			val model = trainWithParams(scaledDataCats, 0.0, param, new SimpleUpdater, 1.0)
			createMetrics(s"$param iterations", scaledDataCats, model)
		}
		iterResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }

		// step size
		val stepResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
			val model = trainWithParams(scaledDataCats, 0.0, numIterations, new SimpleUpdater, param)
			createMetrics(s"$param step size", scaledDataCats, model)
		}
		stepResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }

		// regularization
		val regResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
			val model = trainWithParams(scaledDataCats, param, numIterations, new SquaredL2Updater, 1.0)
			createMetrics(s"$param L2 regularization parameter", scaledDataCats, model)
		}
		regResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }

		def trainDTWithParams(input: RDD[LabeledPoint], maxDepth: Int, impurity: Impurity) = {
			DecisionTree.train(input, Algo.Classification, impurity, maxDepth)
		}

		// investigate tree depth impact for Entropy impurity
		val dtResultsEntropy = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
			val model = trainDTWithParams(data, param, Entropy)
			val scoreAndLabels = data.map { point =>
				val score = model.predict(point.features)
				(if (score > 0.5) 1.0 else 0.0, point.label)
			}
			val metrics = new BinaryClassificationMetrics(scoreAndLabels)
			(s"$param tree depth", metrics.areaUnderROC)
		}
		dtResultsEntropy.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }

		// investigate tree depth impact for Gini impurity
		val dtResultsGini = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
			val model = trainDTWithParams(data, param, Gini)
			val scoreAndLabels = data.map { point =>
				val score = model.predict(point.features)
				(if (score > 0.5) 1.0 else 0.0, point.label)
			}
			val metrics = new BinaryClassificationMetrics(scoreAndLabels)
			(s"$param tree depth", metrics.areaUnderROC)
		}
		dtResultsGini.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }

		// investigate Naive Bayes parameters
		def trainNBWithParams(input: RDD[LabeledPoint], lambda: Double) = {
			val nb = new NaiveBayes
			nb.setLambda(lambda)
			nb.run(input)
		}
		val nbResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
			val model = trainNBWithParams(dataNB, param)
			val scoreAndLabels = dataNB.map { point =>
				(model.predict(point.features), point.label)
			}
			val metrics = new BinaryClassificationMetrics(scoreAndLabels)
			(s"$param lambda", metrics.areaUnderROC)
		}
		nbResults.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }

		// illustrate cross-validation
		// create a 60% / 40% train/test data split
		val trainTestSplit = scaledDataCats.randomSplit(Array(0.6, 0.4), 123)
		val train = trainTestSplit(0)
		val test = trainTestSplit(1)
		// now we train our model using the 'train' dataset, and compute predictions on unseen 'test' data
		// in addition, we will evaluate the differing performance of regularization on training and test datasets
		val regResultsTest = Seq(0.0, 0.001, 0.0025, 0.005, 0.01).map { param =>
			val model = trainWithParams(train, param, numIterations, new SquaredL2Updater, 1.0)
			createMetrics(s"$param L2 regularization parameter", test, model)
		}
		regResultsTest.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.6f%%") }

		// training set results
		val regResultsTrain = Seq(0.0, 0.001, 0.0025, 0.005, 0.01).map { param =>
			val model = trainWithParams(train, param, numIterations, new SquaredL2Updater, 1.0)
			createMetrics(s"$param L2 regularization parameter", train, model)
		}
		regResultsTrain.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.6f%%") }
	}

}