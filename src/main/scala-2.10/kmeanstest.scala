import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

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
  def wdchashmap(sc: SparkContext, txtpath: String): Array[(String, Int)] = {
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
    val linewordcount = line.split(" |=|,|\\.|-|\"").filter(w => w.matches("[a-zA-Z]+")).groupBy(w=>w).toList.map(c => (c._1, c._2.length))
    linewordcount.map {
      wordcount =>
        val index = vec.indexOf(wordcount._1)
        if (index >= 0)
          arrp(index) = wordcount._2 * Math.log(n / 1.0 / hashmap.getOrElse(wordcount._1, 1))
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
    val hashmap = wdchashmap(sc, "hdfs://master:9000/user/bigdata/ips.csv").toMap
    //val hashmap = wdchashmap(sc, "hdfs://master:9000/user/bigdata/ips.csv", vec)
    //hashmap.foreach(println)

    // 将整个日志rdd转化为向量rdd
    val vecrdd = sc.textFile("hdfs://master:9000/user/bigdata/ips.csv")
      .map{line => line2vec(line, vec, hashmap, lines)}

    val kmeansdata = vecrdd.map{
      line =>
        Vectors.dense(line.map(_.toDouble))
    }.cache()

    // 划分为三个子集，最多迭代50次
    val kmeansModel = KMeans.train(kmeansdata, 3, 50)
    // 输出聚类中心
    kmeansModel.clusterCenters.foreach{ println }
    // 计算聚类损失
    val kmeansCost = kmeansModel.computeCost(kmeansdata)
    println("Kmeans cost: " + kmeansCost)

    // 输出每个聚类的索引
    kmeansdata.foreach{
      vec =>
        println(kmeansModel.predict(vec) + ": " + vec)
    }

    // vecrdd.takeSample(false, 3).foreach(sample => println(sample.toList.mkString(",")))

    // 将所有的数据输出，查看正确性
    //println("=========zhu==========")
    //println("文档个数： " + lines)
    //hashmap.foreach(println)

    //val ipsline = """Nov 29 22:05:39 115.156.191.116 date=2016-11-29,time=22:05:33,devname=FG3K6C3A13800359,devid=FG3K6C3A13800359,logid=0000000013,type=traffi   c,subtype=forward,level=notice,vd=vpn,srcip=41.219.52.195,srcport=12260,srcintf=""port28"",dstip=202.114.0.245,dstport=25,dstintf=""port27"",poluuid=ad3bafdc-a   fd0-51e5-1920-e1d9420b2549,sessionid=2207114345,proto=6,action=deny,policyid=15,dstcountry=""China"",srccountry=""Senegal"",trandisp=noop,service=""SMTP"",dura   tion=0,sentbyte=0,rcvdbyte=0,sentpkt=0,appcat=""unscanned"",crscore=30,craction=131072,crlevel=high","2016-11-29T22:05:39.000+0800","115.156.191.116",hust,1,"/   var/log/ips1.log",ips,"myindexer.hust.edu.cn"""
    //val doubline = line2vec(ipsline, vec, hashmap, lines)
    //println(doubline.toList.mkString(","))

    sc.stop()
  }
}
