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
    //flatMapDemo()

    // mapPartitionsWithIndex示例
    mapPartitionsWithIndexDemo()
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
    val rddData = sparkSession.sparkContext.parallelize(Array("a", "b", "c"))
    val data = rddData.mapPartitionsWithIndex((index: Int, row: Iterator[String]) => {
      row.toList.map(x => "[partID:" + index + ":" + x + "]").iterator
    })
    data.collect().foreach(println(_))
  }
}
