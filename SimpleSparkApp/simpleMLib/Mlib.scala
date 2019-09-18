import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

object Mlib{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SimpleMLib")
    val sc = new SparkContext(conf)
    
    val spam = sc.textFile("spam.txt")
    val normal = sc.textFile("ham.txt")

    val tf = new HashingTF(numFeatures = 10000)

    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache()

    val model = new LogisticRegressionWithSGD().run(trainingData)

    val posTest = tf.transform(
      "O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))

    println("positive test Example: " + model.predict(posTest))
    println("negative test Example: " + model.predict(negTest))
  }
}
