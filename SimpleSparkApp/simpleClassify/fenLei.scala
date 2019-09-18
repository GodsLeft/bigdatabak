import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.impurity.Gini

object fenLei{
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("fenLei")
    val sc = new SparkContext(conf)
    
    val records = sc.textFile("../train_noheader.tsv").map(line => line.split("\t"))
    val data = records.map{ r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if(d == "?") 0.0 else d.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }.cache()
    val numData = data.count

    val nbData = records.map{r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if(d == "?") 0.0 else d.toDouble).map(d => if(d < 0) 0.0 else d)
      LabeledPoint(label, Vectors.dense(features))
    }

    val numIterations = 10
    val maxTreeDepth = 5
    val lrModel = LogisticRegressionWithSGD.train(data, numIterations)
    val svmModel = SVMWithSGD.train(data, numIterations)
    val nbModel = NaiveBayes.train(nbData)
    val dtModel = DecisionTree.train(data, Algo.Classification, Entropy, maxTreeDepth)

    val dataPoint = data.first
    val prediction = lrModel.predict(dataPoint.features)
    val trueLabel = dataPoint.label
    println(prediction, trueLabel)
    
    val predictions = lrModel.predict(data.map(lp => lp.features))
    predictions.take(5)

    val lrTotalCorrect = data.map{point =>
      if(lrModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val lrAccuracy = lrTotalCorrect / data.count
    println("="*4 + lrAccuracy + "="*4)

    val svmTotalCorrect = data.map{ point =>
      if(svmModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val nbTotalCorrect = nbData.map{ point =>
      if(nbModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val dtTotalCorrect = data.map{ point =>
      val score = dtModel.predict(point.features)
      val predicted = if(score > 0.5) 1 else 0
      if(predicted == point.label) 1 else 0
    }.sum
    
    val svmAccuracy = svmTotalCorrect / numData
    val nbAccuracy = nbTotalCorrect / numData
    val dtAccuracy = dtTotalCorrect / numData
    println("="*20)
    println(svmAccuracy, nbAccuracy, dtAccuracy)
    println("="*20)

    val metrics = Seq(lrModel, svmModel).map{model =>
      val scoreAndLabels = data.map{ point =>
        (model.predict(point.features), point.label)
      }

      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }

    val nbMetrics = Seq(nbModel).map{ model =>
      val scoreAndLabels = nbData.map{ point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }

      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }

    val dtMetrics = Seq(dtModel).map{model =>
      val scoreAndLabels = data.map{ point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }

    val allMetrics = metrics ++ nbMetrics ++ dtMetrics
    println("="*30)
    allMetrics.foreach{case (m, pr, roc) =>
      println(f"$m, Area under PR: ${pr * 100.0}%2.4f%%, Area under ROC: ${roc * 100}%2.4f%%")
    }
    println("="*30)

    /*
     *接下来，改进性能以及参数调优
     * */
    val vectors = data.map(lp => lp.features)
    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics()
    println("="*30)
    println(matrixSummary.mean)
    println(matrixSummary.min)
    println(matrixSummary.max)
    println(matrixSummary.variance)
    println(matrixSummary.numNonzeros)
    println("="*30)

    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    val scaledData = data.map(lp => LabeledPoint(lp.label, scaler.transform(lp.features)))

    val lrModelScaled = LogisticRegressionWithSGD.train(scaledData, numIterations)
    val lrTotalCorrectScaled = scaledData.map{ point =>
      if(lrModelScaled.predict(point.features) == point.label) 1 else 0
    }.sum

    val lrAccuracyScaled = lrTotalCorrectScaled / numData
    val lrPredictionsVsTrue = scaledData.map{ point =>
      (lrModelScaled.predict(point.features), point.label)
    }
    val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
    val lrPr = lrMetricsScaled.areaUnderPR
    val lrRoc = lrMetricsScaled.areaUnderROC
    println("="*30)
    println(f"${lrModelScaled.getClass.getSimpleName}\nAccuracy:${lrAccuracyScaled * 100}%2.4f%%\nArea under PR: ${lrPr * 100.0}%2.4f%%\nArea under POC: ${lrRoc * 100.0}%2.4f%%")
    println("="*30)

    /*
     * 接下来，添加其他特征
     */
    val categories = records.map(r => r(3)).distinct.collect.zipWithIndex.toMap
    val numCategories = categories.size
    //println(categories)
    val dataCategories = records.map{ r =>
      val trimmed = r.map(_.replaceAll("\"",""))
      val label = trimmed(r.size - 1).toInt
      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0
      val otherFeatures = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      val features = categoryFeatures ++ otherFeatures
      LabeledPoint(label, Vectors.dense(features))
    }
    //println(dataCategories.first)
    val scalerCats = new StandardScaler(withMean = true, withStd = true).fit(dataCategories.map(lp => lp.features))
    val scaledDataCats = dataCategories.map(lp =>
          LabeledPoint(lp.label, scalerCats.transform(lp.features))
        )

    val lrModelScaledCats = LogisticRegressionWithSGD.train(scaledDataCats, numIterations)
    val lrTotalCorrectScaledCats = scaledDataCats.map{point =>
      if(lrModelScaledCats.predict(point.features) == point.label) 1 else 0
    }.sum

    val lrAccuracyScaledCats = lrTotalCorrectScaledCats / numData
    val lrPredictionsVsTrueCats = scaledDataCats.map{point =>
      (lrModelScaledCats.predict(point.features), point.label)
    }
    val lrMetricsScaledCats = new BinaryClassificationMetrics(lrPredictionsVsTrueCats)
    val lrPrCats = lrMetricsScaledCats.areaUnderPR
    val lrRocCats = lrMetricsScaledCats.areaUnderROC
    println(lrAccuracyScaledCats, lrPrCats, lrRocCats)
    /*
     * 使用正确的数据格式
     */
    val dataNB = records.map{r =>
      val trimmed = r.map(_.replaceAll("\"",""))
      val label = trimmed(r.size - 1).toInt
      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0
      LabeledPoint(label, Vectors.dense(categoryFeatures))
    }
    val nbModelCats = NaiveBayes.train(dataNB)
    val nbTotalCorrectCats = dataNB.map{point =>
      if(nbModelCats.predict(point.features) == point.label) 1 else 0
    }.sum

    val nbAccuracyCats = nbTotalCorrectCats / numData
    val nbPredictionsVsTrueCats = dataNB.map{point =>
      (nbModelCats.predict(point.features), point.label)
    }
    val nbMetricsCats = new BinaryClassificationMetrics(nbPredictionsVsTrueCats)
    val nbPrCats = nbMetricsCats.areaUnderPR
    val nbRocCats = nbMetricsCats.areaUnderROC
    println("="*4 + (nbAccuracyCats, nbPrCats, nbRocCats))

    /*
     * 模型参数调优
     */
    def trainWithParams(input: RDD[LabeledPoint], regParam: Double, numIterations: Int, updater: Updater, setpSize: Double)={
      val lr = new LogisticRegressionWithSGD
      lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(setpSize)
      lr.run(input)
    }
    def createMetrics(label: String, data: RDD[LabeledPoint], model:ClassificationModel) = {
      val scoreAndLabels = data.map{point => (model.predict(point.features), point.label)}
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (label, metrics.areaUnderROC)
    }
    scaledDataCats.cache()
    //迭代次数
    val iterResutls = Seq(1, 5, 10, 50).map{ param =>
      val model = trainWithParams(scaledDataCats, 0.0, param, new SimpleUpdater, 1.0)
      createMetrics(s"$param iterations", scaledDataCats, model)
    }
    iterResutls.foreach{ case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    
    //SGD步长
    val stepResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map{ param =>
      val model = trainWithParams(scaledDataCats, 0.0, numIterations, new SimpleUpdater, param)
      createMetrics(s"$param step size", scaledDataCats, model)
    }
    stepResults.foreach{case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%")}

    //正则化
    val regResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map{param =>
      val model = trainWithParams(scaledDataCats, param, numIterations, new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularization parameter", scaledDataCats, model)
    }
    regResults.foreach{case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%")}

    //决策树
    def trainDTWithParams(input: RDD[LabeledPoint], maxDepth: Int, impurity: Impurity) = {
      DecisionTree.train(input, Algo.Classification, impurity, maxDepth)
    }
    val dtResultsEntropy = Seq(1, 2, 3, 4, 5, 10, 20).map{ param =>
      val model = trainDTWithParams(data, param, Entropy)
      val scoreAndLabels = data.map{point =>
        val score = model.predict(point.features)
        (if(score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC)
    }
    dtResultsEntropy.foreach{ case(param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }

    /*
     * 交叉验证
     */
    val trainTestSplit = scaledDataCats.randomSplit(Array(0.6, 0.4), 123)
    val train = trainTestSplit(0)
    val test = trainTestSplit(1)
    val regResultsTest = Seq(0.0, 0.001, 0.0025, 0.005, 0.01).map{param =>
      val model = trainWithParams(train, param, numIterations, new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularization parameter", test, model)
    }
    regResultsTest.foreach{case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.6f%%")}

    sc.stop()
  }
}
