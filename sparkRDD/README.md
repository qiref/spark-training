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

#### Transformation 算子

Transformation 转换操作，将一个RDD转化为另外一个RDD，但是该过程具有延迟加载性；
Transformation算子不会马上执行，只有当遇到Action算子时才会执行。

常见的Transformation算子：

* **map(function)** 由一个RDD转化为另外一个RDD，function的每一次输出组成另外一个RDD；

* **filter(function)** 由一个RDD的元素经过筛选，满足function条件的元素组成一个新的RDD；

* **flatMap(function)** 类似于map，但是每一个元素可以被转化为多个元素，function应该返回一个序列；

```scala
  /**
    * flatMap实例，flatMap返回的是一组元素，官网说是一个Seq ，而不是一个元素
    */
  def flatMapDemo(): Unit = {
    val sparkSession = getDefaultSparkSession
    val textData = sparkSession.sparkContext.textFile("sparkRDD/src/main/resources/data.txt")
    //    val flatData = textData.flatMap(row => row.split(" "))
    val flatData = textData.map(row => row.split(" "))
    flatData.collect().foreach(println(_))
  }
```

* **mapPartitions(function)** 类似于map，但独立地在RDD的每一个分片上运行，函数类型是：Iterator[T] => Iterator[U]；

  传值调用（call-by-value）：先计算参数表达式的值，再应用到函数内部，在函数外部求值；

  传名调用（call-by-name）：将未计算的参数表达式直接应用到函数内部，在函数内部求值；

  Iterator[T] => Iterator[U] 就是表示该函数为传名调用。

* **mapPartitionsWithIndex(function)** 类似于mapPartitions，但是传入的参数中多了一个索引值，该索引值为RDD分片数的索引值；
  （传入的函数类型为：(Int, Iterator<T>) => Iterator<U>）

```scala
  /**
    * mapPartitionsWithIndex使用示例
    */
  def mapPartitionsWithIndexDemo(): Unit = {
    val sparkSession = getDefaultSparkSession
    val rddData = sparkSession.sparkContext.parallelize(Array("a", "b", "c", "d", "e"))
    val data = rddData.mapPartitionsWithIndex((index: Int, row: Iterator[String]) => {
      row.toList.map(x => "[partID:" + index + ":" + x + "]").iterator
    })
    data.collect().foreach(println(_))
  }

```

* **sample(withReplacement, fraction, seed)** 根据fraction指定的比例对数据进行采样，可以选择是否使用随机数进行替换，seed用于指定随机数生成器种子；

* **union(otherDataset)**  对源RDD和参数中的RDD求并集后返回一个新的RDD；

```scala
  /**
    * 求并集示例
    */
  def unionRDD(): Unit = {
    val sparkSession = getDefaultSparkSession
    val rddData1 = sparkSession.sparkContext.parallelize(List("s", "d", "f", "a"))
    val rddData2 = rddData1.map(row => {
      if (row.equals("s")) {
        row + "s"
      } else {
        row
      }
    })
    println("求并集:")
    rddData1.union(rddData2).foreach(println(_))
    println("求交集:")
    rddData1.intersection(rddData2).foreach(println(_))

    val rddData3 = rddData1.union(rddData2)
    println("去重:")
    rddData3.distinct().collect().foreach(println(_))
  }
```

* **intersection(otherDataset)**  对源RDD和参数中的RDD求交集后返回一个新的RDD；

* **distinct([numTasks]))**  对源RDD进行去重后返回一个新的RDD；

* **groupByKey([numTasks])**  在一个(K,V)的RDD上调用，返回一个(K, Iterator[V])的RDD；

```scala
  /**
    * 分组示例
    */
  def groupByKeyDemo(): Unit = {
    val sparkSession = getDefaultSparkSession
    val textFileRDD = sparkSession.sparkContext.textFile("sparkRDD/src/main/resources/data.txt")
    val rddData = textFileRDD.flatMap(_.split(" ")).map(row => (row, 1))
    rddData.groupByKey().map(row => {
      val count = row._2.sum
      (row._1, count)
    }).collect().foreach(println(_))
    rddData.reduceByKey((x, y) => x + y).collect().foreach(println(_))
    //    rddData.groupByKey().collect().foreach(println(_))
  }

```

* **reduceByKey(func, [numTasks])** 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，与groupByKey类似，reduce任务的个数可以通过第二个可选的参数来设置； 与groupByKey的不同在于reduceByKey中可以传入一个函数，处理规约后的每个值；groupByKey则是将分组后的值都放到Iterator中；

  ```scala
  val textFileRDD = sparkSession.sparkContext.textFile("sparkRDD/src/main/resources/data.txt")
      val rddData = textFileRDD.flatMap(_.split(" ")).map(row => (row, 1))
      rddData.groupByKey().map(row => {
        val count = row._2.sum
        (row._1, count)
      }).collect().foreach(println(_))
  
      rddData.reduceByKey((x, y) => x + y).collect().foreach(println(_))
  ```

  看完reduceByKey之后再去看看distinct()的源码，就会发现很有意思：

  ```scala
    def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
      map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
    }
  ```

  先是将一个RDD转化为(x,null) 这种二元结构，然后按照每个key进行规约，这样就能保证key只有一个，而x,y都为null，最后只需要再将规约后的key取出来，就是去重后的RDD了。

* **aggregateByKey (zeroValue)(seqOp, combOp, [numTasks])**  先按分区聚合 ，再总的聚合 ；每次要跟初始值交流 例如：aggregateByKey(0)(_+_,_+_) 对k/y的RDD进行操作；

```scala
  /**
    * 聚合，暂时还没理解
    */
  def aggregateByKeyDemo(): Unit = {
    val sparkSession = getDefaultSparkSession
    val textFileRDD = sparkSession.sparkContext.textFile("sparkRDD/src/main/resources/data.txt")
    val rddData = textFileRDD.flatMap(_.split(" ")).map(row => (row, 1))
    val aggregateRDD = rddData.aggregateByKey(0)(_ + _, _ + _)
    aggregateRDD.collect().foreach(println(_))
  }
```

* **sortByKey([*ascending*], [*numPartitions*])**  在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD；

```scala
  /**
    * 特殊的排序
    */
  def sortByKeyDemo(): Unit = {
    val sparkSession = getDefaultSparkSession
    val rddData2 = sparkSession.sparkContext.parallelize(List("CSDN", "ITEYE", "CNBLOG", "OSCHINA", "GITHUB"))
    val rddData3 = sparkSession.sparkContext.parallelize(1 to rddData2.count().toInt)
    val rddData4 = rddData2.zip(rddData3)
    rddData4.sortByKey().collect().foreach(println(_))
  }
```

* **sortBy(func,[ascending], [numTasks])** 与sortByKey类似，排序的对象也是（K,V）结构，第一个参数是根据什么排序， 第二个是怎么排序 false倒序 ，第三个排序后分区数 ，默认与原RDD一样；

  ```scala
  def sortByDemo(): Unit = {
      val sparkSession = getDefaultSparkSession
      val rddData = sparkSession.sparkContext.parallelize(List(3, 23, 4, 6, 234, 87))
      val newRdd = rddData.mapPartitionsWithIndex((index: Int, row: Iterator[Int]) => {
        row.toList.map(r => (index, r)).iterator
      })
      newRdd.sortBy(_._2, ascending = false).collect().foreach(println(_))
    }
  ```
  
* **join(otherDataset, [numTasks])**  在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD ,相当于内连接（求交集)；

```scala
  /**
    * zip 和 join 操作
    */
  def joinDemo(): Unit = {
    val sparkSession = getDefaultSparkSession
    val rddDataName = sparkSession.sparkContext.parallelize(List("Tom", "Rose", "Jack", "Jerry"))
    val rddDataId = sparkSession.sparkContext.parallelize(List("1001", "1002", "1003", "1004"))
    val rddDataAge = sparkSession.sparkContext.parallelize(List(12, 22, 13, 20))
    val rddIdAndName = rddDataId.zip(rddDataName)
    rddIdAndName.collect().foreach(println(_))
    val rddIdAndAge = rddDataId.zip(rddDataAge)
    rddIdAndAge.collect().foreach(println(_))
    val fullRdd = rddIdAndName.join(rddIdAndAge)
    fullRdd.collect().foreach(println(_))
  }
```
  
* **cogroup(otherDataset, [numTasks])**  在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD；在2.3之后没有该方法。

#### Action 算子

代码中至少有一个Action算子时才会正常执行。

#### 小细节

##### 引用成员变量

以下代码中map中引用了class中的成员变量；

```scala
class MyClass {
  val field = "Hello"
  def doStuff(rdd: RDD[String]): RDD[String] = { rdd.map(x => field + x) }
}
```

但这种方式等价于： ```scala rdd.map(x => this.field + x) ``` ，这种情况会引用整个this；
正确的做法是这样的：

```scala
def doStuff(rdd: RDD[String]): RDD[String] = {
  // 复制一份副本到本地
  val field_ = this.field
  rdd.map(x => field_ + x)
}
```

在map中引用成员变量，应该在进行转换之前就复制一份副本到本地，然后使用本地的副本而不是去引用成员变量；

##### 求和操作

不能在代码中直接使用foreach求和

```scala
var counter = 0
var rdd = sc.parallelize(data)

// Wrong: Don't do this!!
rdd.foreach(x => counter += x)

println("Counter value: " + counter)
```