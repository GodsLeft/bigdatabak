# spark性能测试
- 简单的spark日志分析算法
- sparkbench:https://github.com/SparkTC/spark-bench
- spark-perf

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

## fpgrowth
- 关联规则算法
- 支持度：项集同时含有x，y的概率
- 置信度：在x发生的情况下，y发生的概率，小的置信度筛选可靠的规则
- 在满足一定支持度的情况下寻找置信度达到阀值的所有模式

## someidea
- srcdstip
    + 使用graphviz绘图工具自动生成 srcip->dstip 的图
    + 过滤掉了很多小于阀值的访问请求
    + 画出来的图太大，不好看
    + 其中有一部分ip访问较为频繁，是因为他们是DNS服务器
    + 可以考虑使用流式计算，在一定的时间间隔内生成一个图，多图连续，变为动态图
- streamingdemo
    + 以流式数据来处理
    + 使用sparkstreaming每隔一个时间段生成一个图
    + 先运行流模拟程序:`./streamingmock.sh`
    + 接着运行sparkstreaming:`spark-submit --class someidea.streamingdemo sparktest*.jar 2>/dev/null`
    + 运行图像生成传输程序: `./todot.sh`
    + 运行程序接收程序: `./recivedot.sh`

#### 存在问题
- 生成的动态图不好看，如果能将图片中的位置固定就好了
- 如果能够将ip的地理位置解析出来就更好了
- 可以建立一个数据库类似的东西，内网ip绿色，校内ip黄色，校外ip红色

## 参考文章
- 介绍了文本聚类：http://blog.csdn.net/xiaojimanman/article/details/44977889
- 介绍了kmeans算法：http://www.jianshu.com/p/32e895a940a2
- 介绍了异常检测算法：http://www.jianshu.com/p/d8b6bb53f9a0
