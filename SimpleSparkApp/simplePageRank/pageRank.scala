import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable

object SimplePageRank{
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    var links = sc.parallelize(Array(('A',Array('D')), ('B', Array('A')), ('C', Array('A','B')), ('D', Array('A','C'))),2).map(x=>(x._1, x._2)).cache()
    var ranks = sc.parallelize(Array(('A',1.0), ('B',1.0), ('C',1.0), ('D',1.0)),2)

    for(i <- 1 to 20){
      /*
      val contribs = links.join(ranks, 2)
      val flatMapRdd = contribs.flatMap{ case (url, (links, rank)) => links.map(dest => (dest, rank/links.size))}
      */
/*
      val flatMapRdd = links.join(ranks, 2).flatMap{ case (url, (links, rank)) => links.map(dest => (dest, rank/links.size))}
      val reduceByKeyRdd = flatMapRdd.reduceByKey(_+_, 2)
      val ranks = reduceByKeyRdd.mapValues(0.15 + 0.85 * _)
*/    
      ranks = links.join(ranks, 2)
        .flatMap{ case (url, (links, rank)) => links.map(dest => (dest, rank/links.size))}
        .reduceByKey(_+_, 2)
        .mapValues(0.15 + 0.85 * _)

    }

    val showRanks = ranks.collect()
    showRanks.foreach(println)
  }
}
