package stackoverflow

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    // the lines from the csv file as strings
    // a line has the following structure:
    // <postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]
    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")

    //  the raw Posting entries for each line
    val raw     = rawPostings(lines)

    // questions and answers grouped together
    val grouped = groupedPostings(raw)

    // questions and scores
    val scored  = scoredPostings(grouped)

    // pairs of (language, score) for each question
    val vectors = vectorPostings(scored)

    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(
        postingType =    arr(0).toInt,    // Type of the post. Type 1 = question, type 2 = answer.
        id =             arr(1).toInt,    // Unique id of the post (regardless of type).
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),  // Id of the accepted answer post.
        // this information is optional, so maybe be missing  indicated by an empty string.
        parentId =       if (arr(3) == "") None else Some(arr(3).toInt), // For an answer: id of the corresponding
        // question. For a question:missing, indicated by an empty string.
        score =          arr(4).toInt,  // The StackOverflow score (based on user votes
        tags =           if (arr.length >= 6) Some(arr(5).intern()) else None) // The tag indicates the programming language
      // that the post is about.  in case it's a  question, or missing in case it's an answer.
    })


  /** Group the questions and answers together
    *
    * we want to obtain an RDD with the pairs of (Question, Iterable[Answer]).
    * -> match on the QID, thus producing an RDD[(QID, Iterable[(Question, Answer))].
    * */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {

    val postings2 = postings.cache()
    val questionRdd = postings2.filter(post => post.postingType == 1).map(post => (post.id, post))
    val answerRdd   = postings2.filter(post => post.postingType == 2) // && post.parentId != None)
      .map(post => (post.parentId.get, post))

    /** 03_optimizing-with-partitioners_spark-3-3.pdf
      * todo: manually set partition tie breaks based on binned post ids
      */
    val tunedPartitioner = new RangePartitioner(8, questionRdd)
    val questionRddPart = questionRdd.partitionBy(tunedPartitioner).persist()
    val answerRddPart = answerRdd.partitionBy(tunedPartitioner).persist()

    questionRddPart.join(answerRddPart).partitionBy(tunedPartitioner).persist().groupByKey()
  }


  /** Compute the maximum score for each posting
    *
    * return an RDD containing pairs of (a) questions and (b) the score of the answer
    * with the highest score (note: this does not have to be the answer marked as acceptedAnswer!)
    * */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    grouped.values.map{
      pairs => {

        // unzip splitta le pairs in due collections separate: una per le keys una per i values
        val unzipped = pairs unzip

        // get the questions (they are all the same!)
        val questions = unzipped._1.toArray

        // get the answers
        val answers = unzipped._2.toArray

        // produce the pair: (question, answer with the highest score)
        (questions(0), answerHighScore(answers))
      }
    }
  }


  /** Compute the vectors for the kmeans
    *
    * Transform the scored RDD into a vectors RDD containing the vectors to be clustered
    * Vectors should be pairs with two components
    * - Index of the language multiplied by the langSpread factor.
    * - The highest answer score (computed above).
    *
    * The langSpread factor is provided (set to 50000). It makes sure posts about different programming
    * languages have at least distance 50000 using the distance measure provided by the euclideanDist function
    * */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {

    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    val scoredCached = scored.cache()
    scoredCached
      .map({ case (post, rating) => (firstLangInTag(post.tags, langs), rating) })
      .filter(xs => xs._1.nonEmpty)
      .map({ case (tag, rating) => (tag.get * langSpread, rating) })
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
      // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
      // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }

  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

    val newMeans = means.clone() // you need to compute newMeans

    val vectors2 = vectors.cache()

    /** computing the new means by averaging the vectors that form each cluster
      * Average the vectors
      * averageVectors(ps: Iterable[(Int, Int)]): (Int, Int)
      **/
    vectors2
      .map(xs => (findClosest(xs, newMeans), xs)) // PairRDD[(Int, (Int, Int))]
      .groupByKey.mapValues(averageVectors(_)).collect()
      .foreach({ case (idx, p) => newMeans.update(idx, p) })

    /** euclideanDistance: Return the euclidean distance between two arrays */
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }


  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {

    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>

      // most common language in the cluster
      val langIndex: Int = vs.groupBy(xs => xs._1)
          .map(xs => (xs._1, xs._2.size))
          .maxBy(xs => xs._1)._1 / langSpread

      val langLabel: String   = langs(langIndex)
      val clusterSize: Int    = vs.size

      // percent of the questions in the most common language
      val langPercent: Double = vs.map( { case (v1, v2) => v1 })
          .filter(v1 => v1 == langIndex * langSpread).size * 100 / clusterSize

      val medianScore: Int    = medianVectors(vs.map(_._2.toDouble).toSeq).toInt

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  /** Calculate median for vector */
  def medianVectors(s: Seq[Double]) = {
    val (lower, upper) =
      s.sortWith(_ < _).
        splitAt(s.size / 2)
    if (s.size % 2 == 0)
      (lower.last + upper.head) / 2.0
    else upper.head
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
