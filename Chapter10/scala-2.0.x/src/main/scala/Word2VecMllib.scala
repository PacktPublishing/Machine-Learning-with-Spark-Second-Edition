import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{SparseVector => SV}


/**
 * A simple Spark app in Scala
 */
object Word2VecMllib {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "Word2Vector App")

    val path = "./data/20news-bydate-train/alt.atheism/*"
    val rdd = sc.wholeTextFiles(path)
    val text = rdd.map { case (file, text) => text }
    val newsgroups = rdd.map { case (file, text) => file.split("/").takeRight(2).head }
    val newsgroupsMap = newsgroups.distinct.collect().zipWithIndex.toMap
    val dim = math.pow(2, 18).toInt

    var tokens = text.map(doc => TFIDFExtraction.tokenize(doc))
    import org.apache.spark.mllib.feature.Word2Vec
    val word2vec = new Word2Vec()
    //word2vec.setSeed(42) // we do this to generate the same results each time
    val word2vecModel = word2vec.fit(tokens)
    word2vecModel.findSynonyms("philosophers", 5).foreach(println)

    sc.stop()
  }
}
