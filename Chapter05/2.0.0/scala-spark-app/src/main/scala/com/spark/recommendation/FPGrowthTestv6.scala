package org.apache.spark.examples.mllib

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.reflect.runtime.universe._

object FPGrowthTestv6 {

  case class Params(
    input: String = null,
    minSupport: Double = 0.3,
    numPartition: Int = -1) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("FPGrowthTestv6") {
      head("FPGrowth: an example FP-growth app.")
      opt[Double]("minSupport")
        .text(s"minimal support level, default: ${defaultParams.minSupport}")
        .action((x, c) => c.copy(minSupport = x))
      opt[Int]("numPartition")
        .text(s"number of partition, default: ${defaultParams.numPartition}")
        .action((x, c) => c.copy(numPartition = x))
      arg[String]("<input>")
        .text("input paths to input data set, whose file format is that each line " +
          "contains a transaction with each item in String and separated by a space")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"FPGrowthTestv6 with $params")
    val sc = new SparkContext(conf)
    val transactions = sc.textFile(params.input).map(_.split(" ")).cache()

    println(s"Number of transactions: ${transactions.count()}")

    val model = new FPGrowth()
      .setMinSupport(params.minSupport)
      .setNumPartitions(params.numPartition)
      .run(transactions)

    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    sc.stop()
  }
  abstract class AbstractParams[T: TypeTag] {
    private def tag: TypeTag[T] = typeTag[T]
    /**
      * Finds all case class fields in concrete class instance, and outputs them in JSON-style format:
      * {
      * [field name]:\t[field value]\n
      * [field name]:\t[field value]\n
      * ...
      * }
      */
    override def toString: String = {
      val tpe = tag.tpe
      val allAccessors = tpe.declarations.collect {
        case m: MethodSymbol if m.isCaseAccessor => m
      }
      val mirror = runtimeMirror(getClass.getClassLoader)
      val instanceMirror = mirror.reflect(this)
      allAccessors.map { f =>
        val paramName = f.name.toString
        val fieldMirror = instanceMirror.reflectField(f)
        val paramValue = fieldMirror.get
        s" $paramName:\t$paramValue"
      }.mkString("{\n", ",\n", "\n}")
    }
  }
}
