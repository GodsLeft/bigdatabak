package dataskew

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-5-8.
  */
object groupbykey {
  def main(args: Array[String]): Unit = {
    val inputpath = if(args(0) != null) args(0) else "hdfs://master:9000/user/bigdata/skewdata/"
    val outputpath = if(args(1) != null) args(1) else "hdfs://master:9000/user/bigdata/skewdataout"
    val conf = new SparkConf().setAppName("groupbykey")
    val sc = new SparkContext(conf)

    sc.textFile(inputpath).map{
      line =>
        val index = line.indexOf(',')
        val key = line.substring(0, index)
        val value = line.substring(index+1)
        (key, value)
    }.groupByKey()
      .saveAsTextFile(args(1))

    sc.stop()

  }
}
