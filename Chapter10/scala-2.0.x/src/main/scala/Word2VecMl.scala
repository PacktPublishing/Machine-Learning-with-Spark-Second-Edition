import org.apache.spark.{SparkConf}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object Word2VecMl {
  case class Record(name: String)

  def main(args: Array[String]) {
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder
      .appName("Word2Vec Sample").config(spConfig)
      .getOrCreate()

    import spark.implicits._

    val rawDF = spark.sparkContext
      .wholeTextFiles("./data/20news-bydate-train/alt.atheism/*")

    val temp = rawDF.map( x => {
      (x._2.filter(_ >= ' ').filter(! _.toString.startsWith("(")) )
    })

    val textDF = temp.map(x => x.split(" ")).map(Tuple1.apply)
      .toDF("text")
    print(textDF.first())
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(textDF)
    val result = model.transform(textDF)
    result.select("result").take(3).foreach(println)
    val ds = model.findSynonyms("philosophers", 5).select("word")
    ds.rdd.saveAsTextFile("./output/alien-synonyms" +  System.nanoTime())
    ds.show()
    spark.stop()
  }
}
