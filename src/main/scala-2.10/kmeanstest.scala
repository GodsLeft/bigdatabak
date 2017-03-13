import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by left on 17-3-13.
  */
object kmeanstest {
  // 可以考虑只传递一个Rdd进来
  def wdchashmap(sc: SparkContext, txtpath: String, vec: Array[String]): HashMap[String, Int] = {
    val hashmap = new mutable.HashMap[String, Int]()
    vec.foreach(word => hashmap.put(word, 0))

    val wordcount = sc.textFile(txtpath)
      .map{ line => //一篇文档一篇文档的进行处理
        val linewords = line.split(" |=|,|\\.|\"").filter(word => word.matches("[a-zA-Z]+")).distinct
        linewords.foreach{ //对文档中的每一个词进行处理
          word =>
            if (hashmap.contains(word)){
              hashmap(word) = hashmap.getOrElse(word, 0) + 1
            }
        }
      }
    hashmap
  }

  // 单词：文档个数
  def wdchashmap1(sc: SparkContext, txtpath: String): Array[(String, Int)] = {
    val wordcount = sc.textFile(txtpath)
      .map(line=> line.split(" |=|,|\\.|\"").filter(word => word.matches("[a-zA-Z]+")).distinct)
      .flatMap(arr => arr)
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .collect()
    wordcount
  }
  /**
    * 将一行日志转换为单词向量
    * @param txt 输入的文本
    * @param vec 输入的词集
    * @return 输出对应该文本的向量
    */
  def convertVec(txt: String, vec: Array[String]): Array[Double] = {
    val words = txt.split(" |=|,|\\.|-|\"").filter(word => word.matches("[a-zA-Z]+"))
    val arrp = new Array[Double](vec.size)
    words.foreach{
      word =>
        val index = vec.indexOf(word)
        if (index >=0 ) {
          arrp(index) += 1
        }
    }
    arrp
  }

  // 将一行文本转化为向量，使用了近似的tf-idf算法
  def line2vec(line: String, vec: Array[String], hashmap: Map[String, Int], n: Long): Array[Double] = {
    val arrp = new Array[Double](vec.size)
    // 返回List， 在本行中（单词，个数）
    val linewordcount = line.split(" |=|,|\\.|-|\'").filter(w => w.matches("[a-zA-Z]+")).groupBy(w=>w).toList.map(c => (c._1, c._2.length))
    linewordcount.map {
      wordcount =>
        val index = vec.indexOf(wordcount._1)
        if (index >= 0)
          arrp(index) = wordcount._2 * Math.log(n / hashmap.getOrElse(wordcount._1, 1))
    }
    arrp
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kmeans")
    val sc = new SparkContext(conf)

    // 计算总共有多少个词
    val word = sc.textFile("hdfs://master:9000/user/bigdata/ips.csv")
      .flatMap(line => line.split(" |=|,|\\.|-|\""))
      .filter(word => word.matches("[a-zA-Z]+"))
      .distinct()
      .cache()

    // 文档个数
    val lines = sc.textFile("hdfs://master:9000/user/bigdata/ips.csv").count()

    // 词集
    val vec = word.collect()

    // 单词：含有此单词的文档个数
    val hashmap = wdchashmap1(sc, "hdfs://master:9000/user/bigdata/ips.csv").toMap
    //val hashmap = wdchashmap(sc, "hdfs://master:9000/user/bigdata/ips.csv", vec)
    //hashmap.foreach(println)

    // 将整个日志rdd转化为向量rdd
    val vecrdd = sc.textFile("hdfs://master:9000/user/bigdata/ips.csv")
      .map{line => line2vec(line, vec, hashmap, lines)}

    vecrdd.takeSample(false, 3).foreach(println)

    sc.stop()
  }
}
