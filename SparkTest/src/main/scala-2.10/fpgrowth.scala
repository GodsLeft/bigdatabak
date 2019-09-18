import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by left on 17-3-28.
  */
object fpgrowth {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("fpgrowth")
    val sc = new SparkContext(conf)

    val data = sc.textFile("fpgrowth.txt")

    // 每一行作为一个订单
    val transactions = data.map(s => s.trim.split(' '))

    val fpg = new FPGrowth()
      .setMinSupport(0.2) // 设置支持度
      .setNumPartitions(10)

    val model = fpg.run(transactions)

    // 查看所有的频繁项集，并列出他们出现的次数
    model.freqItemsets.collect().foreach{ itemset =>
      println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq)
    }

    // 通过置信度筛选出推荐规则，
    // 相同规则产生的不同的推荐结果是不同的行，
    // 不同的规则可能会产生相同的推荐
    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach{ rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
        + " => " + rule.consequent.mkString("[", ",", "]")
        + ", " + rule.confidence)
    }
  }
}
