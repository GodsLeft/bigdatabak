package wordcount

import org.apache.spark.{SparkConf, SparkContext}
import util.util

/**
  * Created by left on 17-3-9.
  */

object wordcount {
  /**
    * @param sc
    * @param input:对输入的文件路径
    * @return :输出单词不同单词个数
    */
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
  }
}
