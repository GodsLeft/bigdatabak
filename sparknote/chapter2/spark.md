# spark架构
- https://github.com/JerryLead/SparkInternals/tree/master/markdown
- http://www.cnblogs.com/luogankun/p/3826245.html：这篇文章讲述了DAGScheduler，比较详细,包含了整个DAG的过程
- http://blog.csdn.net/anzhsoft/article/details/42519333：关于spark shuffle Pluggable讲解，还有一系列的Spark shuffle相关的文章
- https://issues.apache.org/jira/browse/SPARK-3280：sort-based shuffle设置为默认的实现
- http://blog.csdn.net/yueqian_zhu/article/details/48368991：shuffle写过程
- http://shiyanjun.cn/archives/744.html:Spark设计论文翻译，很不错

# 非常重要的文章
- https://github.com/linbojin/spark-notes/blob/master/rdd-abstraction.md：**修改源代码，源代码阅读环境**
- https://yq.aliyun.com/articles/64839?spm=5176.100239.blogcont64823.22.eb77IS： 这篇文章在shuffle过程讲的还算比较清晰
- https://www.iteblog.com/archives/1672#HashShuffleManager-3：shuffle调优，关于shuffle参数调优讲的比较清楚,bypass的运行机制
- http://blog.sina.com.cn/s/blog_9ca9623b0102wel4.html：spark源码的文章还行
- https://0x0fff.com/spark-architecture-shuffle/：关于shuffle的图画的挺不错的
- http://www.csdn.net/article/2015-01-12/2823526/2：百度的做法跟我的论文很像


# shuffle过程
- 只有阶段与阶段之间需要shuffle，第一个阶段Map阶段，第二个阶段Reduce阶段。Shuffle是通过Map阶段的ShuffleMapTask与Reduce阶段的ShuffledRDD配合完成的。
- ShuffleMapTask会把任务的计算结果写入ShuffleWriter，ShuffledRDD从ShuffleReader中读取数据，Shuffle过程会在写入和读取过程中完成。
- 以HashShuffle为例，HashShuffleWriter在写入数据时，会决定是否在原分区做聚合，然后根据数据的Hash值写入相应的分区。HashShuffleReader再根据分区号取出相应的数据，然后对数据进行聚合。

![数据读取过程](数据读取.png)

## 明白shuffle的调用关系
### Shuffle结果写入
```scala
ShuffleMapTask.runTask
    HashShuffleWriter.write
        BlockObjectWriter.write
```
### Shuffle结果读取
- ShuffledRDD的compute函数是读取ShuffleMapTask计算结果的触发点
```scala
//ShuffledRDD.compute
override def compute(split: Partition, context: TaskContext): Iterator[P] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index+1, context)
    .read()
    .asInstanceOf[Iterator[P]]
}
```
- shuffleManager.getReader返回的是HashShuffleReader（在1.6.3版本中返回的是BlockStoreShuffleReader）




## 理清一些概念
- 一个shuffleId对应多个mapId

## 想要获取sort shuffle的Index文件
- `IndexShuffleBlockResolver.scala`
- 通过这个函数可以看到，标识一个shuffle阶段产生的数据只需要`shuffleId + mapId`
```scala
//获取index file
blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))

//获取data file
blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
```

## 调用流程
- spark-shell
- spark-submit
- spark-class

## spark核心功能
- 在任何时候都能够重算，是我们为什么把RDD描述为弹性的原因。当保存RDD数据的一台机器失败时，Spark还可以使用这种特性来重算出丢失的分区，这一过程对用户是完全透明的。
- 不要把RDD看成存放数据的数据集，记录如何计算数据的指令列表，更合适
- 本地模式中：spark驱动器程序和各个执行器程序在同一个Java进程中运行。这是一个特例。通常执行器进程会运行在一个单独的专用进程中。
- SparkContext：DriverApplication的执行和输出都是通过SparkContext完成的，在正式提交Application之前，首先需要初始化SparkContext。其隐藏了网络通信、分布式部署、消息通信、存储能力、计算能力、缓存、测量系统、文件服务、web服务等内容，应用开发者只需要使用SparkContext提供的API完成开发功能。
- spark调度器从最终被调用action的RDD出发，向上回溯所有必须计算的RDD。
- Spark混洗操作的输出一定会写入磁盘么？
- stage的划分是从后向前进行的，每当遇到一个shuffle操作，就划分出一个stage
- 每个stage都会有一组完全相同的Task组成
- stage和TaskSet是一一对应的么？
- 两级调度：DAGScheduler作业调度，TaskScheduler任务调度

## 具体执行过程
1、用户程序会创建一个SparkContext，根据用户在编程的时候设置的参数，或者系统默认配置连接到Cluster Master上，Cluster Master根据用户提交时设置的CPU和内存信息，，来为本次程序分配计算资源，启动Executor。

2、Driver会将用户提交来的程序经行Stage级别划分，每个stage就会有一组完全相同的Task组成，在stage具体划分完成和Task具体创建完成之后，Driver会向Executor发送具体的任务Task。

3、Executor在接收到Task之后，会下载Task的运行时依赖的包和库，准备好Task运行环境之后，就会在线程池中开始执行Task，task在运行时会把状态以及结果汇报给Driver

4、Driver会根据收到的Task的运行状态处理不同的状态更新，Task分为两种，一种是Shuffle MapTask它实现数据的重新洗牌，在所有的stage中除了最后一个之外，所有的stage都成为Shuffle阶段，结果会保存在Executor的本地系统文件中，另一种，也就是最后一个stage成为ResultStage：result Task，他负责生成结果数据。

5、Driver会不断的调用Task，将Task发送到Executor执行，在所有的Task都执行正确或者超过执行次数的限制仍然没有执行成功时停止。


## spark Executor
- Executor内部通过线程池的方式来完成Task的计算的
- CoarseGrainedExecutorBackend是一个进程，里面有一个Executor对象，他们两个是一一对应的；
- CoarseGrainedExecutorBackend是一个消息通信体（实现了ThreadSafeRpcEndPoint）。可以发送消息给Driver并可以接受Driver中发过来的指令，如启动Task等。
- 在Driver进程中有两个至关重要的Endpoint
    + ClientEndpoint
    + DriverEndpoint


### DAGScheduler
- 拆分stage
- 记录RDD物化
- 传递taskSet

### TaskScheduler
- 真正的将任务提交到集群去泡
- ScheduleBackend
    + 实现与底层资源调度系统的交互（YARN）
    + 配合TaskScehduler实现具体任务执行所需的资源分配（核心接口receiveOffers）

## spark如何维护RDD之间的依赖关系

## Spark编译打包
```bash
git checkout v1.6.1

mvn -Pyarn -Phadoop-2.6 -Dhadoop.version=2.7.1 -Phive -Phive-thriftserver -Psparkr -DskipTests clean package

#-P指定版本，-D指定具体版本
./make-distribution.sh --name custom-spark --tgz -Psparkr -Phadoop-2.6 -Dhadoop.version=2.7.1 -Phive -Phive-thriftserver -Pyarn
```


## 如何阅读spark的源代码
- 从你最熟悉的看起，比如说RDD
- 从RDD中找几个简单的transformation/action操作
- 找一个含有shuffle操作的RDD函数
- 带着问题去看源代码，如dagScheduler是如何工作的
- RDD如何记录依赖

## spark的使用
### 累加器
- 绝对可靠的累加器：在action中使用累加器
- 在transform中使用的累加器，可能会使得累加器不可靠
- mapPartitions():提供的是分区的迭代器，而不像map()函数提供的是RDD中的每个元素，所以使用mapPartitions的时候，要自己写循环处理迭代器中的每个元素！
- 重新分区使用repartition()函数，该操作会把RDD随机打乱并分成设定的分区数目，如果你要减少RDD分区可以使用coalease()操作，没有打乱数据更高效

### spark-submit
- 如果再调用spark-submit时除了脚本或jar包的名字之外没有别的参数，那么这个Spark程序只会在本地运行。
- 指定--master
    + spark://host:port：连接到指定端口的Spark集群上。
    + mesos://host:port：连接到Mesos集群上
    + yarn
    + local：本地模式，单核
    + local[N]：本地模式，N个核心
    + local[*]：本地模式，尽可能多的核心
- --deploy-mode选择在何处启动驱动器程序:
    + client模式下，驱动器运行在spark-submit被调用的这台机器上，默认是本地模式
    + cluster模式，驱动器运行在集群的一个工作节点上
- --executor-memory:默认值1GB
- --total-executorcores：所有执行器进程所占用的核心总数
- yarn集群可以使用
    + --queue选择队列的名字
    + --executor-cores:每个执行器占用的核心个数
    + --executor-memory:每个执行器使用的内存量
    + --num-executors:固定数量的执行器节点，默认情况为2
- `./spark-submit --master spark://client:7077 --class org.apache.spark.examples.SparkPi lib/spark-examples-1.6.1-hadoop2.6.0.jar`

```bash
./bin/spark-submit \
    --master yarn-cluster \
    --num-executors 100 \
    --executor-memory 6G \
    --executor-cores 4 \
    --driver-memory 1G \
    --conf spark.default.parallelism=1000 \
    --conf spark.storage.memoryFraction=0.5 \
    --conf spark.shuffle.memoryFraction=0.3
```

### spark-class
```bash
#启动主节点
bin/spark-class org.apache.spark.deploy.master.Master

#启动工作节点
bin/spark-class org.apache.spark.deploy.worker.Worker spark://masternode:7077
```

### sparkConf参数设定 (下面案优先级排列)
- 在用户代码中设置
- spark-submit传递参数
- 配置文件中的值
- 默认值
- 几乎所有的配置都发生在SparkConf的创建过程中
- conf/spark-env.sh将环境变量SPARK_LOCAL_DIRS设置为用逗号隔开的存储位置列表，来指定Spark用来混洗数据的本地路径。

### spark-shell
- spark-shell --master spark://masternode:7077
- standlone
    + start-master.sh
    + ./sbin/start-slave.sh <master-spark-url:7077>


## 相关观念校正
- 一个Job包含多个stage，划分stage是根据shuffle过程，但是shuffle并不一定提交作业
- 一个stage有一组taskset，而不是一个操作一组taskset



## 现在遇到的问题
- `DAGScheduler#handleJobSubmitted`函数之后，怎么追踪代码


## Shuffle过程
- https://ihainan.gitbooks.io/spark-source-code/content/section3/index.html
- Shuffle过程中，提供数据的一端称为Map端，Map端每个生成数据的任务称为Mapper
- 接收数据的一端称为Reduce端，每个拉取任务称为Reducer

### MR模型的Shuffle端
- 每个Mapper维护一个环形内存缓冲区，用于存储任务输出，每当内存缓冲区达到一定阀值时候，将缓冲区的数据进行分区(Partition)，对于同一个分区内部数据按照键值进行排序(Sort)，如果指定了Combiner，那么对于排序后的结果，还会执行一次(Combine)操作，最后的结果会被溢存(Spill)到磁盘文件中，在任务完成之前，Apache Hadoop会采用多路归并算法(K-way Merge Algorithm)来归并(Merge)这几个内部有序的溢存文件，新文件中的数据同样是有序的。

![hadoop_shuffle](hadoop_shuffle.png)

- 一个mapper有多个临时溢出文件，每个文件都有多个分区，最终多路归并算法合并成一个文件。
 
- Reducer需要从Map段拉取数据，从Mapper端获取数据，所有需要的数据复制完毕之后，Reducer会合并来自不同Map端拉取过来的数据，并将最后排序好的的数据送往Reducer进行处理，这也是一个多路归并

## 使用maven构建scala项目
- net.alchim31.maven:scala-archetype-simple
- org.scala-tools.archetypes:scala-archetype-simple
- 使用一个打包插件maven-shade-plugin
```xml
<plugins>
    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>version</version>
        <executions>
            <execution>
                <phase>package</phase>
                <goals>
                    <goal>shade</goal>
                </goals>

                <configuration>
                    <transformers>
                        <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                        <!-- <mainClass>com.left.helloworld</mainClass> -->
                        </transformer>
                    </transformers>
                    <createDependencyReducedPom>false</createDependencyReducedPom>
                </configuration>
            </execution>
        </executions>
    </plugin>
</plugins>
```



# 其他
- dirname：取给定目录的路径部分
- basename：取得文件名称部分
```bash
#进入脚本所在的目录
cd $(dirname "$0");
```

## Linux 终端快捷键
```
C + u: 删除光标之前到行首的字符串
C + k：删除光标之前到行尾的字符串
C + a：移动到行首
C + l：清屏
```