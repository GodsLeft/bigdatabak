import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-3-9.
  */

object wordcount {
  def wdcnt(sc: SparkContext, input: String): Long ={
    sc.textFile(input).flatMap(line => line.split(util.regstring))
      .map(w => (w, 1))
      .reduceByKey(_+_)
      .count
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)

    val input = if(args(0) != null) args(0) else util.inputpath
    wdcnt(sc, input)
    sc.stop()
    /*
    val hdfsfile = if (args(0) != null) args(0) else util.inputpath
    val allufile = if (args(1) != null) args(1) else "alluxio://master:19998/user/bigdata/ips.csv"

    val starttime: Long = System.nanoTime();
    wdcnt(sc, hdfsfile)
    val time1 = System.nanoTime() - starttime

    println("============ alluxio start ==========")

    val starttime1: Long = System.nanoTime();
    wdcnt(sc, allufile)
    val time2 = System.nanoTime() - starttime1

    println("zhutime: " + time1 + " : " + time2)
    */

  }
}
