package com.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Author: YaoQi
  * Date: 2019/5/17 17:02
  * Description: 基本RDD操作
  */
object BaseRDDOperator {

  val APP_NAME_DEFAULT = "BASE_RDD_OPERATOR"
  val MASTER_DEFAULT = "local[2]"

  /**
    * 获取sparkSession对象
    *
    * @param appName   应用名称
    * @param master    运行模式
    * @param sparkConf 配置信息
    * @return
    */
  def getSparkSession(appName: String, master: String, sparkConf: SparkConf): SparkSession = {
    val builder = SparkSession.builder()
    if (appName == null || appName.isEmpty) {
      builder.appName(APP_NAME_DEFAULT)
    } else {
      builder.appName(appName)
    }
    if (master == null || master.isEmpty) {
      builder.master(MASTER_DEFAULT)
    } else {
      builder.master(master)
    }
    val sparkSession = builder.config(sparkConf).getOrCreate()
    sparkSession
  }

  /**
    * 获取默认的sparkSession对象
    *
    * @return
    */
  def getDefaultSparkSession: SparkSession = {
    getSparkSession(null, null, new SparkConf())
  }

  def main(args: Array[String]): Unit = {
    // 创建RDD
    // createRdd()

    // 读取一个文件，并统计字符个数
    // charCount()

    // flatMap示例
    // flatMapDemo()

    // mapPartitionsWithIndex示例
    // mapPartitionsWithIndexDemo()

    // 求并集示例
    // unionRDD()

    // 分组示例
    // groupByKeyDemo()

    // 聚合示例
    // aggregateByKeyDemo()

    // 排序实例
    // sortByDemo()

    // 特殊排序
    // sortByKeyDemo()

    // join操作
    // joinDemo()
    coGroupDemo()
  }

  /**
    * 创建一个RDD
    */
  def createRdd(): Unit = {
    val sparkSession = getDefaultSparkSession
    val dataArray = Array(1, 2, 3, 4, 5, 6)
    // 创建一个RDD
    val rdd = sparkSession.sparkContext.parallelize(dataArray, 2)
    // 此处的遍历结果为无序的
    rdd.foreach(row => println(row))
  }

  /**
    * 文件行数统计
    */
  def charCount(): Unit = {
    val sparkSession = getDefaultSparkSession
    // 读取文件转化为一个RDD[String]
    val textData = sparkSession.sparkContext.textFile("sparkRDD/src/main/resources/data.txt")
    val textLines = textData.map(line => line.length)
    val totalLine = textLines.reduce((a, b) => a + b)
    println(totalLine)
  }

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

  /**
    * 排序实例，排序的RDD必须是（K,V）结构
    */
  def sortByDemo(): Unit = {
    val sparkSession = getDefaultSparkSession
    val rddData = sparkSession.sparkContext.parallelize(List(3, 23, 4, 6, 234, 87))
    val newRdd = rddData.mapPartitionsWithIndex((index: Int, row: Iterator[Int]) => {
      row.toList.map(r => (index, r)).iterator
    })
    newRdd.sortBy(_._2, ascending = false).collect().foreach(println(_))
  }

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

}
