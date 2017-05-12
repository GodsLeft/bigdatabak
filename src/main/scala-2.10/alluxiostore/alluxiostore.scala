package alluxiostore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-5-11.
  */
object alluxiostore {
  def main(args: Array[String]): Unit = {

    val inputpath = if (args(0) != null) args(0) else "hdfs://master:9000/user/bigdata/ipsdata/ips_5.csv"
    val linshi = if(args(1) != null) args(1) else "alluxio://master:19998/user/bigdata/linshi"

    val conf = new SparkConf().setAppName("alluxioStore")
    val sc = new SparkContext(conf)

    sc.textFile(inputpath)
      .flatMap(line => line.split(" |=|,|\\.|\""))
      .saveAsTextFile("alluxio://master:19998/user/bigdata/linshi")

    // 将中间结果缓存在alluxio当中
    val word = sc.textFile(linshi)

    // val wordcoutn = word.map(word=>(word, 1)).reduceByKey(_+_).collect()
    val errorcount = word.filter(w => w.contains("error")).count()
    val warncount = word.filter(w => w.contains("warn")).count()
    println("errorcount: " + errorcount)
    println("warncount:  " + warncount)

    sc.stop()
  }
}
