package someidea

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by left on 17-4-2.
  */
object streamingdemo {
  // 暂时不要使用窗口相关的函数
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWorkcount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    val ipreg = """((?:(?:25[0-5]|2[0-4]\d|(?:1\d{2}|[1-9]?\d))\.){3}(?:25[0-5]|2[0-4]\d|(?:1\d{2}|[1-9]?\d)))""".r

    val result = lines
      .map{line =>
        val words = line.split(",")
        val srcips = words.filter(word => word.contains("srcip"))
        val dstips = words.filter(word => word.contains("dstip"))
        if(srcips.length > 0 && dstips.length > 0){
          val srcip = "ip"+ipreg.findFirstIn(srcips(0)).getOrElse("none").replace('.', 'x')
          val dstip = "ip"+ipreg.findFirstIn(dstips(0)).getOrElse("none").replace('.', 'x')
          (srcip + " -> " + dstip, 1)
        } else {
          ("_", 1)
        }
      }
      .reduceByKey(_+_)
      .map{line =>
        val key = line._1
        val value = "[ label = \"" + line._2 + "\" ]"
        key + " " + value + " ;"
      }
      .repartition(1)
        .foreachRDD(_.saveAsTextFile("file:///home/bigdata/streaming"))
      //.saveAsTextFiles(prefix="file:///home/bigdata/streaming", suffix = "dot")


    //val words = lines.flatMap(_.split(","))

    //val pairs = words.map(word => (word, 1))
    //val wordCounts = pairs.reduceByKey(_ + _)
    //wordCounts.print()

    ssc.start() //开始计算
    ssc.awaitTermination() //等待计算终结
  }
}
