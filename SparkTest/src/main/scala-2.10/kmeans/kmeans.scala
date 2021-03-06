package kmeans

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by left on 17-3-13.
  */
object kmeans {
  // 定义分割字符串的方法
  val regstring = util.util.regstring



  // 单词：文档个数，另一种方法，更简单，更快速
  def wdchashmap(sc: SparkContext, txtpath: String): Array[(String, Int)] = {
    val wordcount = sc.textFile(txtpath)
      .map(line=> line.split(regstring).filter(word => word.matches("[a-zA-Z]+")).distinct)
      .flatMap(arr => arr)
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .collect()
    wordcount
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

  def main(args: Array[String]): Unit = {
    println("==========start=========")
    val inputpath = if(args(0)!=null) args(0) else util.util.inputpath
    val outputpath = if(args(1) != null) args(1) else util.util.kmeansout

    val conf = new SparkConf().setAppName("kmeans")
    val sc = new SparkContext(conf)

    // 计算总共有多少个词
    val word = sc.textFile(inputpath)
      .flatMap(line => line.split(regstring))
      .filter(word => word.matches("[a-zA-Z]+"))
      .distinct()
      .cache()

    // 文档个数
    val lines = sc.textFile(inputpath).count()

    // 词集
    val vec = word.collect()

    // 单词：含有此单词的文档个数
    val hashmap = wdchashmap(sc, inputpath).toMap

    // 将整个日志rdd转化为向量rdd
    val vecrdd = sc.textFile(inputpath)
      .map{line => line2vec(line, vec, hashmap, lines)}

    val kmeansdata = vecrdd.map{
      line =>
        Vectors.dense(line.map(_.toDouble))
    }.cache()

    // 划分为三个子集，最多迭代50次
    val centerNum = 3
    val kmeansModel = KMeans.train(kmeansdata, centerNum, 30)
    // 输出聚类中心
    // kmeansModel.clusterCenters.foreach{ println }
    // 计算聚类损失
    val kmeansCost = kmeansModel.computeCost(kmeansdata)
    println("Kmeans cost: " + kmeansCost)

    // 输出每个聚类的索引
    //kmeansdata.foreach{
    //  vec =>
    //    println(kmeansModel.predict(vec) + ": " + vec)
    //}

    // 我要输出原始数据和分类标签
    val result = sc.textFile(inputpath).map{
      line =>
        val linevec = Vectors.dense(line2vec(line, vec, hashmap, lines))
        val label = kmeansModel.predict(linevec)
        (label, line)
    }.cache()

    // 将不同标签的文件输出到不同的文件夹中
    for (label <- 0 until centerNum){
      result.filter( pair => label == pair._1 ).saveAsTextFile(outputpath + "/" + label)
    }

    // vecrdd.takeSample(false, 3).foreach(sample => println(sample.toList.mkString(",")))

    sc.stop()
    println("==========stop=========")
  }
}
