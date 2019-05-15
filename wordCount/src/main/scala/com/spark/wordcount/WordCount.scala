package com.spark.wordcount

import org.apache.spark.sql.SparkSession

/**
  * Description: 单词个数统计
  * Author: YaoQi
  * Date: 2018/9/11 18:12
  */
object WordCount {

  val textFilePath = "wordCount/src/main/resources/data.txt"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("word count").master("local[3]").getOrCreate()
    val fileData1 = spark.sparkContext.textFile(textFilePath)
    val words = fileData1.flatMap(row => row.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey {
      case (x, y) => x + y
    }
    //counts.saveAsTextFile("wordCount/src/main/resources/result.txt")
    counts.foreach(row => {
      println(row._1 + ":" + row._2)
    })
  }
}
