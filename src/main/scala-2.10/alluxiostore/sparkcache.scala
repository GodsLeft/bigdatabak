package alluxiostore

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-5-10.
  */
object alluxiotest {
  def main(args: Array[String]): Unit = {

    val inputpath = if (args(0) != null) args(0) else "hdfs://master:9000/user/bigdata/ipdata/ips_5.csv"

    val conf = new SparkConf().setAppName("memorytest")
    val sc = new SparkContext(conf)

    val word = sc.textFile(inputpath)
      .flatMap(line=>line.split(" |=|,|\\.|\""))
      .cache()

    // val wordcoutn = word.map(word=>(word, 1)).reduceByKey(_+_).collect()
    val errorcount = word.filter(w => w.contains("error")).count()
    val warncount = word.filter(w => w.contains("warn")).count()
    println("errorcount: " + errorcount)
    println("warncount:  " + warncount)

    sc.stop()
  }
}
