import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector
import util.util

/**
  * Created by left on 17-3-29.
  */
object tfidf {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("tf-idf")
    val sc = new SparkContext(conf)

    // 加载文档，每个文档一行
    val docs = sc.textFile("hdfs://master:9000/user/bigdata/ips.csv")
      .map(_.split(util.regstring).filter(w=>w.matches("[a-zA-Z]+")).toSeq)

    val hashingtf = new HashingTF()
    val tf = hashingtf.transform(docs)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    val idfignore = new IDF(minDocFreq = 2).fit(tf)
    val tfidfignore = idfignore.transform(tf)

    println("==========")
    tfidf.take(3).foreach(println)
    println("==========")
    tfidfignore.take(3).foreach(println)
    println("==========")
    println(hashingtf.indexOf("error"))
    println(hashingtf.numFeatures)
  }
}
