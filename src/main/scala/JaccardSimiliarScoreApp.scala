/**
  * Created by bandiboss on 6/18/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Main
  */
object JaccardSimiliarScoreApp {

  /**
  jarcarradIndex using the Set operation
  */

  def jaccardIndex(s1:Set[Int],s2:Set[Int]) = {
    val u = s1.union(s2)
    val i = s1.intersect(s2)
    println("union=" + u.toList)
    println("Intersect=" + i.toList)
    val jcarrdIndex:Float = i.size.toFloat/u.size.toFloat
    println(jcarrdIndex)
  }

  /**
    *
    * @param wordsList
    * @return
    */
  // Jaccard similiarities using List of String, Set(Int) for words
  def jaccard_similarities(wordsList: List[(String,Set[Int])]) = {
    wordsList.combinations(2).map {
      wordpair => (
        (wordpair(0)._1, wordpair(1)._1),
        wordpair(0)._2.intersect(wordpair(1)._2).size / wordpair(0)._2.union(wordpair(1)._2).size.toFloat
      )
    }.toList
  }

  /**
    *
    * @param word_count_map
    * @return
    */
  // Map words counts to Tuple Pair List
  def map_words_count_to_tuple_pair(word_count_map: Map[String,Int]) = {

    var tupleList = ListBuffer[Tuple2[String,Int]]()
    word_count_map.keys.foreach(i => tupleList += Tuple2(i,word_count_map(i)))
    tupleList.toList
  }

  /**
    *
    * @param a
    * @return
    */
  // Map/conver String, Int Tuple Pair to List of [String, Set()]
  def map_tuple_pair_to_List(a: List[Tuple2[String, Int]]) = {
    val b = a.map(t => (t._1, Set(1,t._2)))
    b.toList
  }

  /**
    *
    * @param filename
    * @param sc
    */
  // Process Each File/Doc and compute it is Jaccard Similiarity Score

  def processFile(filename:String, sc:SparkContext): Unit = {
    val simple_path = filename.split('/').last;
    val lines = sc.textFile(filename);
    val word_counts = lines
      .flatMap(line => line.trim.toLowerCase.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _).collect()
 // val fname_word_counts_List = word_counts.map( x => (simple_path,x._1+"\t"+ x._2));   // (filename,word\tcount)
  //println(fname_word_counts_List.toList)

    val tuple_word_pair = map_words_count_to_tuple_pair(word_counts.toMap)
    val tuple_word_list_set = map_tuple_pair_to_List(tuple_word_pair)
    println("-------FileName: " + filename + "--------------")
    println(jaccard_similarities(tuple_word_list_set))
  }

  // Main Function
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("JaccardSimiliarScoreApp")
      .getOrCreate()
    val sc = spark.sparkContext

    val lines = spark.sparkContext.parallelize(
      Seq("London the big red dog is a great good friend",
        "Julie’s favorite animal is a good hot red dog"))

    val counts = lines
      .flatMap(line => line.trim.toLowerCase.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _).collect()

    counts.foreach(println)
    val wordsList = counts.toList

    println(wordsList)
    val tuple_pair = map_words_count_to_tuple_pair(counts.toMap)
    val tuple_list_set = map_tuple_pair_to_List(tuple_pair)
    val s1 = Set(1,2,3)
    val s2 = Set(1,4,5)
    jaccardIndex(s1,s2)

    val p1 = List(("red", Set(1,1)), ("dog", Set(1,2)), ("clifford",Set(1,1)))
    println(jaccard_similarities(tuple_list_set))

    val files = sc.wholeTextFiles("docs").map({case (name, contents) => (name,contents)
      //(name, contents.replaceAll("[^A-Za-z']+", " ").trim.toLowerCase.split("\\s+").toString())
    })
    var lines1: Seq[String] = Seq.empty[String]
    for (x <- files.collect()) {
      lines1 = lines1 :+  x._2.toString
      processFile(x._1,sc)
    }
    println(lines1)
    val lines_combine = sc.parallelize(lines1)

    val word_count_combine = lines_combine.
      flatMap(line => line.trim().split(" ")).
      map(word => (word,1)).
      reduceByKey(_ + _).collect()

    val tuple_pair_combine = map_words_count_to_tuple_pair(word_count_combine.toMap)
    val tuple_list_set_combine = map_tuple_pair_to_List(tuple_pair_combine)

    println(jaccard_similarities(tuple_list_set_combine))
   // files.collect.foreach( (filename,contents) => {processFile(filename,sc)})

  }
}
