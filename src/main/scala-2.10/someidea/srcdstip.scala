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
        val words = line.split(",")
        val srcips = words.filter(word => word.contains("srcip")) //暂时不使用正则表达式
        val dstips = words.filter(word => word.contains("dstip"))
        if(srcips.length>0 && dstips.length>0)
          (srcips(0) + "_" + dstips(0), 1)
        else
          ("_", 1)
      }
      .reduceByKey(_+_)
      .saveAsTextFile("srcdstip")

    sc.stop()
  }
}
