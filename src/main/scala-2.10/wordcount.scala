import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-3-9.
  */

object wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparktest")
    val sc = new SparkContext(conf)

    val starttime: Long = System.nanoTime();
    val logfile = if (args(0) != null) args(0) else "hdfs://master:9000/user/bigdata/ips_10000.csv"
    val file = sc.textFile(logfile)
    val wordcount = file.flatMap(line => line.split(" ")).map(w => (w, 1)).reduceByKey(_+_).count
    val time1 = System.nanoTime() - starttime

    println("============ alluxio start ==========")

    // 使用了alluxio
    val starttime1: Long = System.nanoTime();
    val logfile1 = if (args(1) != null) args(1) else "alluxio://master:19998/user/bigdata/ips_10000.csv"
    val file1 = sc.textFile(logfile1)
    val wordcount1 = file.flatMap(line => line.split(" ")).map(w => (w, 1)).reduceByKey(_+_).count
    val time2 = System.nanoTime() - starttime1

    println("zhutime:" + time1 + " : " + time2)
  }
}
