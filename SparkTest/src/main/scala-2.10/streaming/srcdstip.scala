package streaming

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-4-1.
  */
object srcdstip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("srcdstip")
    val sc = new SparkContext(conf)

    // 正则表达式匹配ip
    val iptn = "srcip=[0-9]+(?:\\.[0-9]+){0,3}".r
    val ipreg = """((?:(?:25[0-5]|2[0-4]\d|(?:1\d{2}|[1-9]?\d))\.){3}(?:25[0-5]|2[0-4]\d|(?:1\d{2}|[1-9]?\d)))""".r

    val txt = sc.textFile("hdfs://master:9000/user/bigdata/ips.csv")
      .map{line =>
        val words = line.split(",")
        val srcips = words.filter(word => word.contains("srcip")) //暂时不使用正则表达式
        val dstips = words.filter(word => word.contains("dstip"))
        if(srcips.length>0 && dstips.length>0) {
          //val srcip = "ip" + srcips(0).split("=")(1).replace('.', 'x')
          //val dstip = "ip" + dstips(0).split("=")(1).replace('.', 'x')
          val srcip = "ip" + ipreg.findFirstIn(srcips(0)).getOrElse("none").replace('.', 'x')
          val dstip = "ip" + ipreg.findFirstIn(dstips(0)).getOrElse("none").replace('.', 'x')
          (srcip + " -> " + dstip, 1)
        }else {
          ("_", 1)
        }
      }
      .reduceByKey(_+_, 1)
      .filter(w => w._2 > 1500) // 过滤连接次数超过10保留
      .map{line =>
        val key = line._1
        val value = "[ label = \"" + line._2 + "\" ]"
        key+" " + value + " ;"
      }
      .saveAsTextFile("srcdstip")

    sc.stop()
  }
}
