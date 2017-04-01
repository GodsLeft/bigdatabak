package someidea

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-4-1.
  */
object srcdstip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("srcdstip")
    val sc = new SparkContext(conf)
    val iptn = "srcip=[0-9]+(?:\\.[0-9]+){0,3}".r

    val txt = sc.textFile("hdfs://master:9000/user/bigdata/ips.csv")
      .map{line =>
        if(line.contains("srcip=")) {
          val words = line.split(",")
          val srcip = words.filter(word => word.contains("srcip"))(0) //暂时不使用正则表达式
          val dstip = words.filter(word => word.contains("dstip"))(0)
          val key = srcip + "_" + dstip
          (key, 1)
        }
        ("_", 1)
      }
      .reduceByKey(_+_)
      .saveAsTextFile("srcdstip")

    sc.stop()
  }
}
