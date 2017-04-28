package org.sparksamples.classification.stumbleupon

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by manpreet.singh on 25/04/16.
  */
object StumbleUponExecutor {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    val conf = SparkCommonUtils.createSparkConf("StumbleUpon")
    val sc = new SparkContext(conf)

    // create sql context
    val sqlContext = new SQLContext(sc)

    // get dataframe
    val df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true")
      .option("inferSchema", "true").load("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/spark-ml/Chapter_06/2.0.0/scala-spark-app/src/main/scala/org/sparksamples/classification/dataset/stumbleupon/train.tsv")

    // pre-processing
    df.registerTempTable("StumbleUpon")
    df.printSchema()
    sqlContext.sql("SELECT * FROM StumbleUpon WHERE alchemy_category = '?'").show()

    // convert type of '4 to n' columns to double
    val df1 = df.withColumn("avglinksize", df("avglinksize").cast("double"))
      .withColumn("commonlinkratio_1", df("commonlinkratio_1").cast("double"))
      .withColumn("commonlinkratio_2", df("commonlinkratio_2").cast("double"))
      .withColumn("commonlinkratio_3", df("commonlinkratio_3").cast("double"))
      .withColumn("commonlinkratio_4", df("commonlinkratio_4").cast("double"))
      .withColumn("compression_ratio", df("compression_ratio").cast("double"))
      .withColumn("embed_ratio", df("embed_ratio").cast("double"))
      .withColumn("framebased", df("framebased").cast("double"))
      .withColumn("frameTagRatio", df("frameTagRatio").cast("double"))
      .withColumn("hasDomainLink", df("hasDomainLink").cast("double"))
      .withColumn("html_ratio", df("html_ratio").cast("double"))
      .withColumn("image_ratio", df("image_ratio").cast("double"))
      .withColumn("is_news", df("is_news").cast("double"))
      .withColumn("lengthyLinkDomain", df("lengthyLinkDomain").cast("double"))
      .withColumn("linkwordscore", df("linkwordscore").cast("double"))
      .withColumn("news_front_page", df("news_front_page").cast("double"))
      .withColumn("non_markup_alphanum_characters", df("non_markup_alphanum_characters").cast("double"))
      .withColumn("numberOfLinks", df("numberOfLinks").cast("double"))
      .withColumn("numwords_in_url", df("numwords_in_url").cast("double"))
      .withColumn("parametrizedLinkRatio", df("parametrizedLinkRatio").cast("double"))
      .withColumn("spelling_errors_ratio", df("spelling_errors_ratio").cast("double"))
      .withColumn("label", df("label").cast("double"))
    df1.printSchema()

    // user defined function for cleanup of ?
    val replacefunc = udf {(x:Double) => if(x == "?") 0.0 else x}

    val df2 = df1.withColumn("avglinksize", replacefunc(df1("avglinksize")))
      .withColumn("commonlinkratio_1", replacefunc(df1("commonlinkratio_1")))
      .withColumn("commonlinkratio_2", replacefunc(df1("commonlinkratio_2")))
      .withColumn("commonlinkratio_3", replacefunc(df1("commonlinkratio_3")))
      .withColumn("commonlinkratio_4", replacefunc(df1("commonlinkratio_4")))
      .withColumn("compression_ratio", replacefunc(df1("compression_ratio")))
      .withColumn("embed_ratio", replacefunc(df1("embed_ratio")))
      .withColumn("framebased", replacefunc(df1("framebased")))
      .withColumn("frameTagRatio", replacefunc(df1("frameTagRatio")))
      .withColumn("hasDomainLink", replacefunc(df1("hasDomainLink")))
      .withColumn("html_ratio", replacefunc(df1("html_ratio")))
      .withColumn("image_ratio", replacefunc(df1("image_ratio")))
      .withColumn("is_news", replacefunc(df1("is_news")))
      .withColumn("lengthyLinkDomain", replacefunc(df1("lengthyLinkDomain")))
      .withColumn("linkwordscore", replacefunc(df1("linkwordscore")))
      .withColumn("news_front_page", replacefunc(df1("news_front_page")))
      .withColumn("non_markup_alphanum_characters", replacefunc(df1("non_markup_alphanum_characters")))
      .withColumn("numberOfLinks", replacefunc(df1("numberOfLinks")))
      .withColumn("numwords_in_url", replacefunc(df1("numwords_in_url")))
      .withColumn("parametrizedLinkRatio", replacefunc(df1("parametrizedLinkRatio")))
      .withColumn("spelling_errors_ratio", replacefunc(df1("spelling_errors_ratio")))
      .withColumn("label", replacefunc(df1("label")))

    // drop first 4 columns
    val df3 = df2.drop("url").drop("urlid").drop("boilerplate").drop("alchemy_category").drop("alchemy_category_score")

    // fill null values with
    val df4 = df3.na.fill(0.0)

    df4.registerTempTable("StumbleUpon_PreProc")
    df4.printSchema()
    sqlContext.sql("SELECT * FROM StumbleUpon_PreProc").show()

    // setup pipeline
    val assembler = new VectorAssembler()
      .setInputCols(Array("avglinksize", "commonlinkratio_1", "commonlinkratio_2", "commonlinkratio_3", "commonlinkratio_4", "compression_ratio"
        , "embed_ratio", "framebased", "frameTagRatio", "hasDomainLink", "html_ratio", "image_ratio"
        ,"is_news", "lengthyLinkDomain", "linkwordscore", "news_front_page", "non_markup_alphanum_characters", "numberOfLinks"
        ,"numwords_in_url", "parametrizedLinkRatio", "spelling_errors_ratio"))
      .setOutputCol("features")

    val command = args(0)

    if(command.equals("NB")) {
      val df5 = prepareForNaiveBayes(df4)

      val nbAssembler = new VectorAssembler()
        .setInputCols(Array("avglinksize", "commonlinkratio_1", "commonlinkratio_2", "commonlinkratio_3", "commonlinkratio_4", "compression_ratio"
          , "embed_ratio", "framebased", "frameTagRatio", "hasDomainLink", "html_ratio", "image_ratio"
          ,"is_news", "lengthyLinkDomain", "linkwordscore", "news_front_page", "non_markup_alphanum_characters", "numberOfLinks"
          ,"numwords_in_url", "parametrizedLinkRatio", "spelling_errors_ratio"))
        .setOutputCol("features")

      executeCommand(command, nbAssembler, df5, sc)
    } else
      executeCommand(command, assembler, df4, sc)
  }

  def executeCommand(arg: String, vectorAssembler: VectorAssembler, dataFrame: DataFrame, sparkContext: SparkContext) = arg match {
    case "LR" => LogisticRegressionPipeline.logisticRegressionPipeline(vectorAssembler, dataFrame)

    case "DT" => DecisionTreePipeline.decisionTreePipeline(vectorAssembler, dataFrame)

    case "RF" => RandomForestPipeline.randomForestPipeline(vectorAssembler, dataFrame)

    case "GBT" => GradientBoostedTreePipeline.gradientBoostedTreePipeline(vectorAssembler, dataFrame)

    case "NB" => NaiveBayesPipeline.naiveBayesPipeline(vectorAssembler, dataFrame)

    case "SVM" => SVMPipeline.svmPipeline(sparkContext)
  }

  def prepareForNaiveBayes(dataFrame: DataFrame): DataFrame = {
    // user defined function for cleanup of ?
    val replacefunc = udf {(x:Double) => if(x < 0) 0.0 else x}

    val df5 = dataFrame.withColumn("avglinksize", replacefunc(dataFrame("avglinksize")))
      .withColumn("commonlinkratio_1", replacefunc(dataFrame("commonlinkratio_1")))
      .withColumn("commonlinkratio_2", replacefunc(dataFrame("commonlinkratio_2")))
      .withColumn("commonlinkratio_3", replacefunc(dataFrame("commonlinkratio_3")))
      .withColumn("commonlinkratio_4", replacefunc(dataFrame("commonlinkratio_4")))
      .withColumn("compression_ratio", replacefunc(dataFrame("compression_ratio")))
      .withColumn("embed_ratio", replacefunc(dataFrame("embed_ratio")))
      .withColumn("framebased", replacefunc(dataFrame("framebased")))
      .withColumn("frameTagRatio", replacefunc(dataFrame("frameTagRatio")))
      .withColumn("hasDomainLink", replacefunc(dataFrame("hasDomainLink")))
      .withColumn("html_ratio", replacefunc(dataFrame("html_ratio")))
      .withColumn("image_ratio", replacefunc(dataFrame("image_ratio")))
      .withColumn("is_news", replacefunc(dataFrame("is_news")))
      .withColumn("lengthyLinkDomain", replacefunc(dataFrame("lengthyLinkDomain")))
      .withColumn("linkwordscore", replacefunc(dataFrame("linkwordscore")))
      .withColumn("news_front_page", replacefunc(dataFrame("news_front_page")))
      .withColumn("non_markup_alphanum_characters", replacefunc(dataFrame("non_markup_alphanum_characters")))
      .withColumn("numberOfLinks", replacefunc(dataFrame("numberOfLinks")))
      .withColumn("numwords_in_url", replacefunc(dataFrame("numwords_in_url")))
      .withColumn("parametrizedLinkRatio", replacefunc(dataFrame("parametrizedLinkRatio")))
      .withColumn("spelling_errors_ratio", replacefunc(dataFrame("spelling_errors_ratio")))
      .withColumn("label", replacefunc(dataFrame("label")))

    return df5
  }

  object DFHelper
  def castColumnTo( df: DataFrame, cn: String, tpe: DataType ) : DataFrame = {
    df.withColumn( cn, df(cn).cast(tpe) )
  }
}

