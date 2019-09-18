import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner

object SimplePageRank{
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)
/*
    val links = sc.textFile("hdfs://master:9000/user/left/cite75_99.txt")
      .filter(line => !line.contains("\""))
      .map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt)) //这样写调用了两次split函数
      .cache()//应该分区之后再进行cache
*/

    val links = sc.textFile("hdfs://master:9000/user/left/cite75_99.txt")
      .filter(line => !line.contains("\""))
      .map{line => 
          val arr = line.split(",")
          (arr(0), arr(1))
      }
      .groupByKey()    //将重复的收集到一个数组中
      .partitionBy(new HashPartitioner(100))
      .cache();

    var ranks = links.map(x => (x._1, 1.0));

    for(i <- 1 to 2){
      ranks = links.join(ranks)
        .flatMap{ case (url, (links, rank)) => links.map(dest => (dest, rank/links.size))}
        .reduceByKey(_+_)
        .mapValues(0.15 + 0.85 * _);
    }

    val showRanks = ranks.take(100)
    showRanks.foreach(println)
  }
}
