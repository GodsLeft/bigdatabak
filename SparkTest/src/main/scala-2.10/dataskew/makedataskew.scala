package dataskew

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-5-8.
  * 将数据变为倾斜数据 9：1 和 5：5
  * 要尽量避免使用groupByKey，容易导致内存溢出
  */
object makedataskew {
  def main(args: Array[String]): Unit = {

    //val datapath = if(args(0) != null) args(0) else "hdfs://master:9000/user/bigdata/ipsdata/ips_2.csv"
    // the all file input path
    // val datapath = "hdfs://master:9000/user/bigdata/ipsdata/ips_5.csv"
    val datapath = "ipsdata/ips_5.csv"
    val ratio = args(0).split(":").map(_.toInt)
    val ratiosum = ratio.sum
    val perclist = ratio.scan(0)(_+_).map(_.toDouble/ratiosum)
    val rationum = perclist.length
    val keylist = "abcdefghigklmnopqrstuvwxyz".toCharArray.map(_.toString)

    val conf = new SparkConf().setAppName("makedataskew")
    val sc = new SparkContext(conf)

    val txt = sc.textFile(datapath)

    val skewrdd = txt.mapPartitions{
      partition =>
        var res = List[(String, String)]()
        val uu = new Random()
        while(partition.hasNext){
          val line = partition.next()
          val rdm = uu.nextFloat()
          for(num <- 1 until rationum) {
            if (rdm > perclist(num-1) && rdm < perclist(num)) {
              res.::=(keylist(num), line)
            }
          }
        }
        res.iterator
    }

    skewrdd.map(line => line._1 + "," + line._2).saveAsTextFile(args(1))

    sc.stop()
  }
}
