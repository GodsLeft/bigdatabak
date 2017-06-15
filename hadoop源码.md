# hadoop源码目录
## 关于如何看源代码的一点感想
- 关注与你要解决的问题
- 比如你要写一个自定义的输出格式，就可以去看TextOutputFormat.java

## 书
- 再买hadoop的书已经没用了，剩下的就要多实践，或者看源码


## 宽依赖和窄依赖
- 更高效的故障恢复：丢失的父RDD分区只需要少量重新计算即可恢复，宽依赖要等到所有的父RDD的数据形成之后才能进行下一步计算
- 宽依赖一个节点故障可能导致所有父RDD的分区丢失，需要完全重新执行，因此宽依赖的RDD会在持有父分区的节点上持久化中间数据，以便故障还原
- 宽依赖的RDD之间需要进行Shuffle操作
- 窄依赖：允许在单个集群节点上流水线式执行，可以逐个元素执行filter和map操作
- 宽依赖：需要所有的父RDD数据可用并且数据已经通过shuffle完成.

# RDD特性
- RDD在任何时候都不需要物化

# 相关的一些问题
1.为什么称RDD提供了粗粒度变换

- 不能够操作某一个元素，只能对整个RDD进行变换



# spark源码相关文章
- 论文： Resilient Distributed Datasets: A Fault-tolerant Abstraction for In-Memory Cluster Computing 
- spark源码第一篇： http://www.csdn.net/article/2014-05-29/2820013/2 
- spark源码：http://blog.csdn.net/huwenfeng_2011/article/details/43344361
- shuffle的好文章：http://www.cnblogs.com/yangsy0915/p/5528774.html
- http://blog.csdn.net/u012684933/article/details/49074185
- spark源码：https://github.com/jacksu/utils4s?hmsr=toutiao.io&utm_medium=toutiao.io&utm_source=toutiao.io
- scala教程：https://www.zhihu.com/question/38836382/answer/79794319?hmsr=toutiao.io&utm_medium=toutiao.io&utm_source=toutiao.io

# 其他
- 在写感悟的时候，不要翻书或者照抄，要自己合上书，回顾，列出不熟悉的点，重新感受再写。
- 我经历了各种挫折，保留运气，只为了用尽所有的运气，遇到最心爱的你
