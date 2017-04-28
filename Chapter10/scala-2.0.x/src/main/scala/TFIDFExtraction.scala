import org.apache.spark.SparkContext

/**
 * A simple Spark app in Scala
 */
object TFIDFExtraction {
  var rareTokens = Set("")
  val stopwords = Set(
    "the","a","an","of","or","in","for","by","on","but", "is", "not", "with", "as", "was", "if",
    "they", "are", "this", "and", "it", "have", "from", "at", "my", "be", "that", "to"
  )
  val regex = """[^0-9]*""".r

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")

    val path = "../data/20news-bydate-train/*"
    val rdd = sc.wholeTextFiles(path)
    // count the number of records in the dataset
    println(rdd.count)
    /*
    ...
    11314
    */
    println(rdd.first())
    val newsgroups = rdd.map { case (file, text) => file.split("/").takeRight(2).head }
    println(newsgroups.first())
    val countByGroup = newsgroups.map(n => (n, 1)).reduceByKey(_ + _).collect.sortBy(-_._2).mkString("\n")
    println(countByGroup)
    /*
    (rec.sport.hockey,600)
    (soc.religion.christian,599)
    (rec.motorcycles,598)
    (rec.sport.baseball,597)
    (sci.crypt,595)
    (rec.autos,594)
    (sci.med,594)
    (comp.windows.x,593)
    (sci.space,593)
    (sci.electronics,591)
    (comp.os.ms-windows.misc,591)
    (comp.sys.ibm.pc.hardware,590)
    (misc.forsale,585)
    (comp.graphics,584)
    (comp.sys.mac.hardware,578)
    (talk.politics.mideast,564)
    (talk.politics.guns,546)
    (alt.atheism,480)
    (talk.politics.misc,465)
    (talk.religion.misc,377)
    */

    val text = rdd.map { case (file, text) => text }
    val whiteSpaceSplit = text.flatMap(t => t.split(" ").map(_.toLowerCase))
    println(whiteSpaceSplit.distinct.count)
    // 402978
    // split text on any non-word tokens
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase))
    println(nonWordSplit.distinct.count)
    // 130126
    // inspect a look at a sample of tokens
    println(nonWordSplit.distinct.sample(true, 0.3, 50).take(100).mkString(","))

    // filter out numbers

    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches)
    println(filterNumbers.distinct.count)
    // 84912
    println(filterNumbers.distinct.sample(true, 0.3, 50).take(100).mkString(","))
    /*
    reunion,wuair,schwabam,eer,silikian,fuller,sloppiness,crying,crying,beckmans,leymarie,fowl,husky,rlhzrlhz,ignore,
    loyalists,goofed,arius,isgal,dfuller,neurologists,robin,jxicaijp,majorly,nondiscriminatory,akl,sively,adultery,
    urtfi,kielbasa,ao,instantaneous,subscriptions,collins,collins,za_,za_,jmckinney,nonmeasurable,nonmeasurable,
    seetex,kjvar,dcbq,randall_clark,theoreticians,theoreticians,congresswoman,sparcstaton,diccon,nonnemacher,
    arresed,ets,sganet,internship,bombay,keysym,newsserver,connecters,igpp,aichi,impute,impute,raffle,nixdorf,
    nixdorf,amazement,butterfield,geosync,geosync,scoliosis,eng,eng,eng,kjznkh,explorers,antisemites,bombardments,
    abba,caramate,tully,mishandles,wgtn,springer,nkm,nkm,alchoholic,chq,shutdown,bruncati,nowadays,mtearle,eastre,
    discernible,bacteriophage,paradijs,systematically,rluap,rluap,blown,moderates
    */

    // examine potential stopwords
    val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)
    val oreringDesc = Ordering.by[(String, Int), Int](_._2)
    println(tokenCounts.top(20)(oreringDesc).mkString("\n"))
    /*
    (the,146532)
    (to,75064)
    (of,69034)
    (a,64195)
    (ax,62406)
    (and,57957)
    (i,53036)
    (in,49402)
    (is,43480)
    (that,39264)
    (it,33638)
    (for,28600)
    (you,26682)
    (from,22670)
    (s,22337)
    (edu,21321)
    (on,20493)
    (this,20121)
    (be,19285)
    (t,18728)
    */
    // filter out stopwords

    val tokenCountsFilteredStopwords = tokenCounts.filter { case (k, v) => !stopwords.contains(k) }
    println(tokenCountsFilteredStopwords.top(20)(oreringDesc).mkString("\n"))
    /*
    (ax,62406)
    (i,53036)
    (you,26682)
    (s,22337)
    (edu,21321)
    (t,18728)
    (m,12756)
    (subject,12264)
    (com,12133)
    (lines,11835)
    (can,11355)
    (organization,11233)
    (re,10534)
    (what,9861)
    (there,9689)
    (x,9332)
    (all,9310)
    (will,9279)
    (we,9227)
    (one,9008)
    */
    // filter out tokens less than 2 characters
    val tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter { case (k, v) => k.size >= 2 }
    println(tokenCountsFilteredSize.top(20)(oreringDesc).mkString("\n"))
    /*
    (ax,62406)
    (you,26682)
    (edu,21321)
    (subject,12264)
    (com,12133)
    (lines,11835)
    (can,11355)
    (organization,11233)
    (re,10534)
    (what,9861)
    (there,9689)
    (all,9310)
    (will,9279)
    (we,9227)
    (one,9008)
    (would,8905)
    (do,8674)
    (he,8441)
    (about,8336)
    (writes,7844)
    */

    // examine tokens with least occurrence
    val oreringAsc = Ordering.by[(String, Int), Int](-_._2)
    println(tokenCountsFilteredSize.top(20)(oreringAsc).mkString("\n"))
    /*
    (lennips,1)
    (bluffing,1)
    (preload,1)
    (altina,1)
    (dan_jacobson,1)
    (vno,1)
    (actu,1)
    (donnalyn,1)
    (ydag,1)
    (mirosoft,1)
    (xiconfiywindow,1)
    (harger,1)
    (feh,1)
    (bankruptcies,1)
    (uncompression,1)
    (d_nibby,1)
    (bunuel,1)
    (odf,1)
    (swith,1)
    (lantastic,1)
    */
    // filter out rare tokens with total occurence < 2
    rareTokens = tokenCounts.filter{ case (k, v) => v < 2 }.map { case (k, v) => k }.collect.toSet
    val tokenCountsFilteredAll = tokenCountsFilteredSize.filter { case (k, v) => !rareTokens.contains(k) }
    println(tokenCountsFilteredAll.top(20)(oreringAsc).mkString("\n"))
    /*
    (sina,2)
    (akachhy,2)
    (mvd,2)
    (hizbolah,2)
    (wendel_clark,2)
    (sarkis,2)
    (purposeful,2)
    (feagans,2)
    (wout,2)
    (uneven,2)
    (senna,2)
    (multimeters,2)
    (bushy,2)
    (subdivided,2)
    (coretest,2)
    (oww,2)
    (historicity,2)
    (mmg,2)
    (margitan,2)
    (defiance,2)
    */
    println(tokenCountsFilteredAll.count)
    // 51801
    // create a function to tokenize each document

    // check that our tokenizer achieves the same result as all the steps above
    println(text.flatMap(doc => tokenize(doc)).distinct.count)
    // 51801
    // tokenize each document
    var tokens = text.map(doc => tokenize(doc))

    println(tokens.first.take(20))
    println(text.flatMap(doc => tokenize(doc)).distinct.count)

    import org.apache.spark.mllib.linalg.{ SparseVector => SV }
    import org.apache.spark.mllib.feature.HashingTF
    import org.apache.spark.mllib.feature.IDF
    // set the dimensionality of TF-IDF vectors to 2^18
    val dim = math.pow(2, 18).toInt
    val hashingTF = new HashingTF(dim)

    val tf = hashingTF.transform(tokens)
    // cache data in memory
    tf.cache
    val v = tf.first.asInstanceOf[SV]
    println(v.size)
    // 262144
    println(v.values.size)
    // 706
    println(v.values.take(10).toSeq)
    // WrappedArray(1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 2.0, 1.0, 1.0)
    println(v.indices.take(10).toSeq)
    // WrappedArray(313, 713, 871, 1202, 1203, 1209, 1795, 1862, 3115, 3166)

    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    val v2 = tfidf.first.asInstanceOf[SV]
    println(v2.values.size)
    // 706
    println(v2.values.take(10).toSeq)
    // WrappedArray(2.3869085659322193, 4.670445463955571, 6.561295835827856, 4.597686109673142,  ...
    println(v2.indices.take(10).toSeq)
    // WrappedArray(313, 713, 871, 1202, 1203, 1209, 1795, 1862, 3115, 3166)

    val minMaxVals = tfidf.map { v =>
      val sv = v.asInstanceOf[SV]
      (sv.values.min, sv.values.max)
    }
    val globalMinMax = minMaxVals.reduce { case ((min1, max1), (min2, max2)) =>
      (math.min(min1, min2), math.max(max1, max2))
    }

    println(globalMinMax)
    // (0.0,66155.39470409753)

    // test out a few common words
    val common = sc.parallelize(Seq(Seq("you", "do", "we")))
    val tfCommon = hashingTF.transform(common)
    val tfidfCommon = idf.transform(tfCommon)
    val commonVector = tfidfCommon.first.asInstanceOf[SV]
    println(commonVector.values.toSeq)
    // WrappedArray(0.9965359935704624, 1.3348773448236835, 0.5457486182039175)

    // test out a few uncommon words
    val uncommon = sc.parallelize(Seq(Seq("telescope", "legislation", "investment")))
    val tfUncommon = hashingTF.transform(uncommon)
    val tfidfUncommon = idf.transform(tfUncommon)
    val uncommonVector = tfidfUncommon.first.asInstanceOf[SV]
    println(uncommonVector.values.toSeq)

    val hockeyText = rdd.filter { case (file, text) => file.contains("hockey") }
    // note that the 'transform' method used below is the one which works on a single document
    // in the form of a Seq[String], rather than the version which works on an RDD of documents
    val hockeyTF = hockeyText.mapValues(doc => hashingTF.transform(tokenize(doc)))
    val hockeyTfIdf = idf.transform(hockeyTF.map(_._2))

    // compute cosine similarity using Breeze
    import breeze.linalg._
    val hockey1 = hockeyTfIdf.sample(true, 0.1, 42).first.asInstanceOf[SV]
    val breeze1 = new SparseVector(hockey1.indices, hockey1.values, hockey1.size)
    val hockey2 = hockeyTfIdf.sample(true, 0.1, 43).first.asInstanceOf[SV]
    val breeze2 = new SparseVector(hockey2.indices, hockey2.values, hockey2.size)
    val cosineSim = breeze1.dot(breeze2) / (norm(breeze1) * norm(breeze2))
    println(cosineSim)

    // 0.06700095047242809

    val graphicsText = rdd.filter { case (file, text) => file.contains("comp.graphics") }
    val graphicsTF = graphicsText.mapValues(doc => hashingTF.transform(tokenize(doc)))
    val graphicsTfIdf = idf.transform(graphicsTF.map(_._2))
    val graphics = graphicsTfIdf.sample(true, 0.1, 42).first.asInstanceOf[SV]
    val breezeGraphics = new SparseVector(graphics.indices, graphics.values, graphics.size)
    val cosineSim2 = breeze1.dot(breezeGraphics) / (norm(breeze1) * norm(breezeGraphics))
    println(cosineSim2)
    // 0.001950124251275256

    // compare to sport.baseball topic
    val baseballText = rdd.filter { case (file, text) => file.contains("baseball") }
    val baseballTF = baseballText.mapValues(doc => hashingTF.transform(tokenize(doc)))
    val baseballTfIdf = idf.transform(baseballTF.map(_._2))
    val baseball = baseballTfIdf.sample(true, 0.1, 42).first.asInstanceOf[SV]
    val breezeBaseball = new SparseVector(baseball.indices, baseball.values, baseball.size)
    val cosineSim3 = breeze1.dot(breezeBaseball) / (norm(breeze1) * norm(breezeBaseball))
    println(cosineSim3)
    // 0.0013298577308832765

    sc.stop()
  }

  def tokenize(line: String): Seq[String] = {
    line.split("""\W+""")
      .map(_.toLowerCase)
      .filter(token => regex.pattern.matcher(token).matches)
      .filterNot(token => stopwords.contains(token))
      .filterNot(token => rareTokens.contains(token))
      .filter(token => token.size >= 2)
      .toSeq
  }

}
