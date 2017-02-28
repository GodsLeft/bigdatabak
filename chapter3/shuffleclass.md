## shuffle过程所涉及到的类
- 用于存储map输出的内存为：mapMeomory = JVM Heap Size * `spark.shuffle.memeoryFraction(0.2)` * `spark.shuffle.safetyFraction(0.8)`
- 如果同一个执行程序中运行多个线程，每个map任务的空间为：mapMemory/`spark.executor.cores` * `spark.task.cpus` 

### ShuffleManager
- Driver和每个Executor都会持有一个ShuffleManager，这个ShuffleManager可以通过配置项spark.shuffle.manager指定，并且由SparkEnv创建。Driver中的ShuffleManager负责注册Shuffle元数据，比如Shuffle ID， map task的数据量等。Executor中的ShuffleManager则负责读和写Shuffle的数据。

### ShuffleBlockResolver
- IndexShuffleBlockResolver通常用于获取Block索引文件，并根据索引文件读取Block文件数据

### FileShuffleBlockResolver
### IndexShuffleBlockResolver
- 通常用于获取block索引文件，并根据索引文件读取block文件数据

### DiskBlockObjectWriter
 
### ShuffleWriter
### HashShuffleWriter
### BlockStoreShuffleReader

### MapStatus
- `org.apache.spark.scheduler`
```scala
private[spark] sealed trait MapStatus {
    def location: BlockManagerId
    def getSizeForBlock(reduceId: Int): Long
}
```

### BlockManagerId
```scala
class BlockManagerId private (
    private var executorId_ : String,
    private var host_ : String,
    private var port_ : Int) extends Exernalizable {}
```


### ShuffleHandle
```scala
abstract class ShuffleHandle(val shuffleId: Int) extends Serializable {}
```


### BaseShuffleHandle
- 一个基本的ShuffleHandle实现，仅仅为了获得registerShuffle的参数
```scala
private[spark] class BaseShuffleHandle[K, V, C](
    shuffleId: Int,
    val numMaps: Int,
    val dependency: ShuffleDependency[K, V, C]) extends ShuffleHandle(shuffleId)
```

### ShuffleDependency
### Dependency
```scala
//只定义了一个函数，返回一个RDD对象
abstract class Dependency[T] extends Serializable{
    def rdd: RDD[T]
}
```
### ShuffleDependency
```scala
class ShuffleDependency[k: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false) extends Dependency[Product2[K, V]]{
        ...
    val shuffleId: Int = _rdd.context.newShuffleId()
    }
```

### ExternalSorter
```scala

```
### BlockStoreShuffleReader




### ShuffleId
### ShuffleBlockId

### TaskContext

### ShuffleMapTask
- 数据的写入过程
- `ShuffleMapTask.runTask`

### ShuffledRDD.compute
- 数据的读取过程


### BypassMergeSortShuffleWriter
- reducer的个数小于`spark.shuffle.sort.bypassMergeThreshold`使用hash相关数据到分开的文件，然后合并这些文件为一个
- 它的实现`org.apaache.spark.shuffle.sort.BypassMergeSortShuffleWriter`


### UnsafeShuffleWriter

### MemoryBlock.java
- 代表了一个Page对象

## Executor的内存
- 主要分为三块：
   + 第一块让task执行我们自己编写的代码时使用，默认占Executor总内存的20%
   + 第二块让task通过shuffle过程拉取了上一个stage的task的输出之后，进行聚合等操作时使用。默认20%
   + 第三块RDD持久化使用，60%



# Tungsten-sort based Shuffle
- http://www.open-open.com/lib/view/open1454288089089.html

对内存/CPU/Cache使用做了很大的优化。如果Tungsten-sort发现自己无法处理，则会自动使用Sort Based Shuffle进行处理

- 三点优化：
    + 直接在serialized binary data上sort而不是java object，减少了memory的开销和GC的overhead
    + 提供cache-efficient sorter，使用一个8bytes的指针，把排序转化成了一个指针数组的排序。
    + spill的merge过程也无需反序列化即可完成

这个优化引入了一个新的内存管理模型，类似OS的Page，对应的实际数据结构为MemoryBlock，支持off-heap以及in-heap两种模式。为了能够对Record在这些MemoryBlock进行定位，引入了Pointer的概念

Sort Based Shuffle里存储数据的对象PartitionedAppendOnlyMap，这是一个放在JVM heap里普通对象，在Tungsten-sort中，他被替换成了类似操作系统内存页的对象。如果你无法申请到Page，这个时候就要执行spill操作，也就是写入到磁盘的操作。具体触发条件和Sort Based Shuffle也是类似的。

## 开启条件
Spark默认开启的是Sort Based Shuffle，想要打开Tungsten-sort，请设置
`spark.shuffle.manager=tungsten-sort`
对应的实现类是：
`org.apache.spark.shuffle.unsafe.UnsafeShuffleManager`
使用了大量的JDK Sun Unsafe API

当且仅当下面条件都满足时，才会使用新的Shuffle方式：

- Shuffle dependency不能带有aggregation或者输出需要排序
- Shuffle的序列化需要是KryoSerializer或者Spark SQL's自定义的一些序列化方式
- 序列化时单条记录不能大于128MB
- Shuffle的文件数量不能大于16777216