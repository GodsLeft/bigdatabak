import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-3-15.
  */
object anomalydetection {

  // 公式中使用到的sigma是标准差
  def gaosi(x: Double, u: Double, sigma: Double): Double = {
    // 1.0 / sigma
    1 / sigma / Math.sqrt(2 * Math.PI) * Math.exp(- Math.pow(x - u, 2) / 2 / Math.pow(sigma, 2))
  }

  // 这边传递的是方差数组
  def linegaosi(xarr: Array[Double], uarr: Array[Double], sarr: Array[Double]): Double = {
    var result = 1.0
    result *= gaosi(xarr(0), uarr(0), sarr(0))
    result
    //for(i <- 0 until xarr.length){
    //  result *= gaosi(xarr(i), uarr(i), sarr(i))
    //}
    //result
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("statistics")
    val sc = new SparkContext(conf)

    val word = sc.textFile(kmeanstest.inputpath)
      .flatMap(line => line.split(kmeanstest.regstring))
      .filter(word => word.matches("[a-zA-Z]+"))
      .distinct()

    val lines = sc.textFile(kmeanstest.inputpath).count()

    val vecbag = word.collect()

    val hashmap = kmeanstest.wdchashmap(sc, kmeanstest.inputpath).toMap

    val vecrdd = sc.textFile(kmeanstest.inputpath).map{line => Vectors.dense(kmeanstest.line2vec(line, vecbag, hashmap, lines))}

    val stat = Statistics.colStats(vecrdd)

    val mean = stat.mean
    val variance = stat.variance
    val sigma = variance.toArray.map(x => Math.sqrt(x))

    // 对任一条数据，进行预测是否异常
    // 暂时只输出异常的
    sc.textFile(kmeanstest.inputpath).map{
      line =>
        // val linevec = Vectors.dense(kmeanstest.line2vec(line, vecbag, hashmap, lines))
        val linevec = kmeanstest.line2vec(line, vecbag, hashmap, lines)
        val lineresult = linegaosi(linevec, mean.toArray, sigma)
        //if (lineresult < 0.001) {
        //  ("a", line)
        //}
        lineresult + " : " + linevec.mkString(",")
    }.saveAsTextFile("hdfs://master:9000/user/bigdata/yichang")

    println("mean: " + mean)
    println("vari: " + variance)
    println("sigm: " + sigma.mkString(","))
  }
}
