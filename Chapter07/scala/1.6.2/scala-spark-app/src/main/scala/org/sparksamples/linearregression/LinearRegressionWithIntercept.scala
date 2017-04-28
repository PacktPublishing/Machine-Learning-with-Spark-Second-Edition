package org.sparksamples.linearregression

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.sparksamples.Util

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * LogisticalRegression App
  * @author Rajdeep Dua
  */
object LinearRegressionWithIntercept{

  def main(args: Array[String]) {
    val recordsArray = Util.getRecords()
    val records = recordsArray._1
    val first = records.first()
    val numData = recordsArray._2

    println(numData.toString())
    records.cache()
    print("Mapping of first categorical feature column: " +  Util.get_mapping(records, 2))
    var list = new ListBuffer[Map[String, Long]]()
    for( i <- 2 to 9){
      val m =  Util.get_mapping(records, i)
      list += m
    }
    val mappings = list.toList
    var catLen = 0
    mappings.foreach( m => (catLen +=m.size))

    val numLen = records.first().slice(11, 15).size
    val totalLen = catLen + numLen

    print("Feature vector length for categorical features:"+ catLen)
    print("Feature vector length for numerical features:" + numLen)
    print("Total feature vector length: " + totalLen)

    val data = {
      records.map(r => LabeledPoint(Util.extractLabel(r), Util.extractFeatures(r, catLen, mappings)))
    }
    val data1 = {
      records.map(r => Util.extractFeatures(r, catLen, mappings))
    }
    val first_point = data.first()
    println("Linear Model feature vector:" + first_point.features.toString)
    println("Linear Model feature vector length: " + first_point.features.size)

    val iterations = 10
    val step = 0.025
    val intercept =true

    val linReg = new LinearRegressionWithSGD().setIntercept(intercept)
    linReg.optimizer.setNumIterations(iterations).setStepSize(step)
    val linear_model = linReg.run(data)
    print(data.first());
    val x = linear_model.predict(data.first().features)
    val true_vs_predicted = data.map(p => (p.label, linear_model.predict(p.features)))
    val true_vs_predicted_csv = data.map(p => p.label + " ,"  + linear_model.predict(p.features))
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
    val date = format.format(new java.util.Date())
    val save = true
    if (save){
      true_vs_predicted_csv.saveAsTextFile("./output/linear_model_" + date + ".csv")
    }
    val true_vs_predicted_take5 = true_vs_predicted.take(5)
    for(i <- 0 until 5) {
      println("True vs Predicted: " + "i :" + true_vs_predicted_take5(i))
    }
    val mse = true_vs_predicted.map{ case(t, p) => Util.squaredError(t, p)}.mean()
    val mae = true_vs_predicted.map{ case(t, p) => Util.absError(t, p)}.mean()
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())

    println("Linear Model - Mean Squared Error: "  + mse)
    println("Linear Model - Mean Absolute Error: " + mae)
    println("Linear Model - Root Mean Squared Log Error:" + rmsle)

  }

}
