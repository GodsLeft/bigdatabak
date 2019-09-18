import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}
import util.util
/**
  * Created by left on 17-3-15.
  */
object statistics {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("streaming")
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
        util.linegaosi(line.toArray, summary.mean.toArray, sigma)
    }

    result.collect().foreach(println)
    //sc.stop()

    // 将数据标准化
    println("=========zhu=========")
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(obver)
    val scalerdata = obver.map(line => scaler.transform(line)).collect()
    scalerdata.foreach(println)

  }
}
