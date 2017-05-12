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

    val pairdd = sc.textFile(inputpath).map{
      line =>
        val index = line.indexOf(',')
        val key = line.substring(0, index)
        val value = line.substring(index+1).length
        (key, value)
    }.groupByKey(3)
      .map{
        pair=>
          val iter = pair._2.toIterator
          var sum = 0.0
          while(iter.hasNext){
            sum += Math.pow(iter.next().toDouble, 2)
          }
          (pair._1, sum)
      }

    pairdd.collect().foreach(println)

    //val pairdd = sc.textFile(inputpath).map{
    //  line =>
    //    val pair = line.split(",")
    //    val value = pair(1).toFloat
    //    (pair(0), value)
    //}
    //  .reduceByKey(_+_)

    //pairdd.collect().foreach(println)

    pairdd.saveAsTextFile(args(1))

    sc.stop()

  }
}
