import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-3-15.
  */
object anomalydetection {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("anomalydetection")
    val sc = new SparkContext(conf)

    val inputpath = if (args(0) != null) args(0) else util.inputpath
    val outputpath = if (args(1) != null) args(1) else util.anoout

    val word = sc.textFile(inputpath)
      .flatMap(line => line.split(util.regstring))
      .filter(word => word.matches("[a-zA-Z]+"))
      .distinct()

    val lines = sc.textFile(inputpath).count()

    val vecbag = word.collect()

    val hashmap = kmeans.wdchashmap(sc, inputpath).toMap

    val vecrdd = sc.textFile(inputpath).map{line => Vectors.dense(util.line2vec(line, vecbag, hashmap, lines))}

    val stat = Statistics.colStats(vecrdd)

    // 没有标准化的均值和方差
    val mean = stat.mean
    val variance = stat.variance
    val sigma = variance.toArray.map(x => Math.sqrt(x))
    println("mean: " + mean)
    println("vari: " + variance)
    println("sigm: " + sigma.mkString(","))

    // 数据标准化
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vecrdd)
    val vectorsrdd = sc.textFile(inputpath).map{
      line =>
        val linevec = util.line2vec(line, vecbag, hashmap, lines)
        scaler.transform(Vectors.dense(linevec))
    }

    // 标准化之后的均值和方差
    println("==========zhu==========")
    val statis = Statistics.colStats(vectorsrdd)
    val mean1 = statis.mean
    val varia = statis.variance
    val sigma1 = varia.toArray.map(x=>Math.sqrt(x))
    println("mean: " + mean1)
    println("vari: " + varia)
    println("sigm: " + sigma1.mkString(","))

    // 将高斯函数值为0的日志输出
    sc.textFile(inputpath).map{
      line =>
        val linevec = util.line2vec(line, vecbag, hashmap, lines)
        val lineresult = util.linegaosi(scaler.transform(Vectors.dense(linevec)).toArray, mean1.toArray, sigma1)
        (lineresult, line)
    }
      .filter(s => s._1 < Double.MinPositiveValue) //要选择一个好的阀值，从1e60开始降低:40:500M 20:466M 10:420M 1:419M
      .saveAsTextFile(outputpath)
  }
}
