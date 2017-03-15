import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-3-15.
  */
object someidea {
  def gaosi(x: Double, u: Double, sigma: Double): Double = {
    1 / sigma / Math.sqrt(2 * Math.PI) * Math.exp(- Math.pow(x - u, 2) / 2 / Math.pow(sigma, 2))
  }


  def linegaosi(xarr: Array[Double], uarr: Array[Double], sarr: Array[Double]): Double = {
    var result = 1.0
    result *= gaosi(xarr(0), uarr(0), sarr(0))
    result
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("someidea")
    val sc = new SparkContext(conf)

    val obver = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    )
    val summary = Statistics.colStats(obver)
    println(summary.mean)
    println(summary.variance)
    println(summary.numNonzeros)
    val sigma = summary.variance.toArray.map(x=>Math.sqrt(x))


    val result = obver.map{
      line =>
        linegaosi(line.toArray, summary.mean.toArray, sigma)
    }

    result.collect().foreach(println)
  }
}
