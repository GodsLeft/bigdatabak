import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.evaluation.RankingMetrics

object Recomd{
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("Recomd")
    val sc = new SparkContext(conf)

    val rawData = sc.textFile("/home/left/ml-100k/u.data")
    val rawRatings = rawData.map(_.split("\t").take(3))
    val ratings = rawRatings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }

    val model = ALS.train(ratings, 50, 10, 0.01)
    //model.userFeature.count
    //model.productFeatures.count

    val predictedRating = model.predict(789, 123)

    //为用户789推荐的前10个商品
    val topKRecs = model.recommendProducts(789, 10)
    println(topKRecs.mkString("\n"))

    //为了检验效果，可以查看用户评价过的电影和被推荐的电影名称
    val movies = sc.textFile("/home/left/ml-100k/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    val moviesForUser = ratings.keyBy(_.user).lookup(789)
    //评级最高的前十部电影
    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    
    //对用户的前十的推荐
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)

    println("=====================")
    
    //物品推荐
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }
    val itemFactor = model.productFeatures.lookup(567).head
    val itemVector = new DoubleMatrix(itemFactor)
    cosineSimilarity(itemVector, itemVector)  //计算自己的相似度
    val sims = model.productFeatures.map{ case (id, factor) => 
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    val sortedSims = sims.top(10)(Ordering.by[(Int, Double), Double]{case (id, similarity) => similarity})
    println(sortedSims.take(10).mkString("\n"))
    println(titles(567))
    val sortedSims2 = sims.top(11)(Ordering.by[(Int, Double), Double]{ case (id, similarity) => similarity })
    sortedSims2.slice(1, 11).map{case (id, sim) => (titles(id), sim)}.mkString("\n")

    //模型评估
    //均方差
    val userProducts = ratings.map{case Rating(user, product, rating) => (user, product)}
    val predictions = model.predict(userProducts).map{
      case Rating(user, product, rating) => ((user, product), rating)
    }

    val ratingsAndPredictions = ratings.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions)

    val MSE = ratingsAndPredictions.map{
      case((user, product), (actual, predicted)) => math.pow((actual - predicted), 2)
    }.reduce(_+_) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)

    //K值平均准确率
    def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int):Double = {
      val predK = predicted.take(k)
      var score = 0.0
      var numHits = 0.0
      for((p, i) <- predK.zipWithIndex){
        if(actual.contains(p)){
          numHits += 1.0
          score += numHits / (i.toDouble + 1.0)
        }
      }
      if(actual.isEmpty){
        1.0
      }else{
        score / scala.math.min(actual.size, k).toDouble
      }
    }
    //用户实际评级过的电影
    val actualMovies = moviesForUser.map(_.product)
    val predictedMovies = topKRecs.map(_.product)//推荐物品列表
    val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)
    println("====\n"+apk10)

    val itemFactors = model.productFeatures.map{case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)
    val imBroadcast = sc.broadcast(itemMatrix)
    val allRecs = model.userFeatures.map{case (userId, array) => 
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedIds = sortedWithId.map(_._2 + 1).toSeq
      (userId, recommendedIds)
    }

    val userMovies = ratings.map{ case Rating(user, product, rating) => (user, product) }.groupBy(_._1)
    val K = 10
    val MAPK = allRecs.join(userMovies).map{case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, K)
    }.reduce(_+_) / allRecs.count
    println("Mean Average Predicted at K = " + MAPK)


    //使用MLlib的评估函数
    val predictedAndTrue = ratingsAndPredictions.map{case ((user, product), (predicted, actual)) => (predicted, actual)}
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
    
    val predictedAndTrueForRanking = allRecs.join(userMovies).map{case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray, actual.toArray)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)

    val MAPK2000 = allRecs.join(userMovies).map{case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, 2000)
    }.reduce(_+_)/allRecs.count
    println("Mean Average Precision = "+ MAPK2000)

    sc.stop()
  }
}
