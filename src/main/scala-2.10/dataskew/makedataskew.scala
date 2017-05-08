package dataskew

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-5-8.
  * 将数据变为倾斜数据 9：1 和 5：5
  */
object makedataskew {
  def main(args: Array[String]): Unit = {

    //val datapath = if(args(0) != null) args(0) else "hdfs://master:9000/user/bigdata/ipsdata/ips_2.csv"
    val datapath = "hdfs://master:9000/user/bigdata/ipsdata/ips_5.csv"
    val perc = args(0).toFloat

    val conf = new SparkConf().setAppName("makedataskew")
    val sc = new SparkContext(conf)

    val txt = sc.textFile(datapath)
    val skewrdd = txt.mapPartitions{
      partition =>
        var res = List[(String, String)]()
        val uu = new Random()
        while(partition.hasNext){
          val line = partition.next()
          if(uu.nextFloat() < perc){
            res.::=("aa", line)
          } else {
            res.::=("bb", line)
          }
        }
        res.iterator
    }

    skewrdd.map(line => line._1 + "," + line._2).saveAsTextFile(args(1))
  }
}
