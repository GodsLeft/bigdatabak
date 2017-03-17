# spark性能测试
- 简单的spark日志分析算法
- sparkbench:https://github.com/SparkTC/spark-bench

## wordcount
- 只是简单的统计单词

## kmeans
- 对日志进行聚类
- 使用用了类似tf-idf算法计算单词向量
- 使用kmeans算法进行聚类

## anomalydetection
- 使用了异常检测算法
- 同样使用了tf-idf算法计算单词的权重
- 并没有使用多元高斯分布，因为每个单词的特征可以认为是独立的
- 关于异常检测的阀值还可以深入考虑
- 另外可以考虑使用word2vec算法

## 参考文章
- 介绍了文本聚类：http://blog.csdn.net/xiaojimanman/article/details/44977889
- 介绍了kmeans算法：http://www.jianshu.com/p/32e895a940a2
- 介绍了异常检测算法：http://www.jianshu.com/p/d8b6bb53f9a0
