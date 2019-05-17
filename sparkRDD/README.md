## 总结

RDD(Resilient Distributed Dataset) 弹性分布式数据集，是一组分布式的数据集合，里面的元素可并行计算，可分区；
RDD允许用户在执行多个查询时显示地将工作集缓存在内存中，例如persist()；

### 创建方式
创建RDD的两种方式：
* 读取外界文件

外界文件不局限于系统文件，包括HDFS、HBase等
```scala
sparkSession.sparkContext.textFile("sparkRDD/src/main/resources/data.txt")
```

* 通过并行化的方式创建

```scala
val sparkSession = getDefaultSparkSession
val dataArray = Array(1, 2, 3, 4, 5, 6)
// 创建一个RDD
val rdd = sparkSession.sparkContext.parallelize(dataArray)
```

通过并行化的方式创建还可以指定分区的数量

```scala
/** Distribute a local Scala collection to form an RDD.
   *
   * @note Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
   * to parallelize and before the first action on the RDD, the resultant RDD will reflect the
   * modified collection. Pass a copy of the argument to avoid this.
   * @note avoid using `parallelize(Seq())` to create an empty `RDD`. Consider `emptyRDD` for an
   * RDD with no partitions, or `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
   * @param seq Scala collection to distribute
   * @param numSlices number of partitions to divide the collection into
   * @return RDD representing distributed collection
   */
  def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    assertNotStopped()
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }
```

### RDD编程

RDD中包含两种类型的算子：Transformation和Action；

#### Transformation

Transformation 转换操作，将一个RDD转化为另外一个RDD，但是该过程具有延迟加载性；
Transformation算子不会马上执行，只有当遇到Action算子时才会执行。

常见的Transformation算子：

* map(function) 由一个RDD转化为另外一个RDD，function的每一次输出组成另外一个RDD。
* filter(function) 由一个RDD的元素经过筛选，满足function条件的元素组成一个新的RDD。
* flatMap(function) 类似于map，但是每一个元素可以被转化为多个元素，function应该返回一个序列。
* mapPartitions(function) 类似于map，但独立地在RDD的每一个分片上运行，函数类型是：Iterator[T] => Iterator[U]

#### Action

代码中至少有一个Action算子时才会正常执行。

