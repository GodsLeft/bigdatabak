package util

import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by left on 17-3-16.
  */
object util {
  // 定义分割字符串的方法
  val regstring = " |=|,|\\.|\""
  //val inputpath = "hdfs://master:9000/user/bigdata/ips.csv"
  //val anoout = "hdfs://master:9000/user/bigdata/yichang"
  val kmeansout = "hdfs://master:9000/user/bigdata/kmeans"

  val inputpath = "alluxio://master:19998/user/bigdata/ips.csv"
  val anoout = "alluxio://master:19998/user/bigdata/yichang"

  // 公式中使用到的sigma是标准差
  def gaosi(x: Double, u: Double, sigma: Double): Double = {
    1 / sigma / Math.sqrt(2 * Math.PI) * Math.exp(- Math.pow(x - u, 2) / 2 / Math.pow(sigma, 2))
  }

  // 这边传递的是方差数组
  def linegaosi(xarr: Array[Double], uarr: Array[Double], sarr: Array[Double]): Double = {
    var result = 1.0
    for(i <- 0 until xarr.length){
      result *= gaosi(xarr(i), uarr(i), sarr(i))
    }
    result
  }

  // 将一行文本转化为向量，使用了近似的tf-idf算法
  def line2vec(line: String, vec: Array[String], hashmap: Map[String, Int], n: Long): Array[Double] = {
    val arrp = new Array[Double](vec.size)
    // 返回List， 在本行中（单词，个数）
    val linewordcount = line.split(regstring).filter(w => w.matches("[a-zA-Z]+")).groupBy(w=>w).toList.map(c => (c._1, c._2.length))
    linewordcount.map {
      wordcount =>
        val index = vec.indexOf(wordcount._1)
        if (index >= 0)
          arrp(index) = wordcount._2 * Math.log(n / 1.0 / hashmap.getOrElse(wordcount._1, 1))
    }
    arrp
  }

  // 可以考虑只传递一个Rdd进来，返回（单词：文档个数）
  def wdchashmap(sc: SparkContext, txtpath: String, vec: Array[String]): HashMap[String, Int] = {
    val hashmap = new mutable.HashMap[String, Int]()
    vec.foreach(word => hashmap.put(word, 0))

    val wordcount = sc.textFile(txtpath)
      .map{ line => //一篇文档一篇文档的进行处理
        val linewords = line.split(regstring).filter(word => word.matches("[a-zA-Z]+")).distinct
        linewords.foreach{ //对文档中的每一个词进行处理
          word =>
            if (hashmap.contains(word)){
              hashmap(word) = hashmap.getOrElse(word, 0) + 1
            }
        }
      }
    hashmap
  }

  /**
  * 将一行日志转换为单词向量
  * @param txt 输入的文本
  * @param vec 输入的词集
  * @return 输出对应该文本的向量
  */
  def convertVec(txt: String, vec: Array[String]): Array[Double] = {
    val words = txt.split(regstring).filter(word => word.matches("[a-zA-Z]+"))
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
}
