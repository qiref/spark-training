package spark.training.wordcount

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
    val count = words.map(word => (word, 1)).reduceByKey((x, y) => x + y)
    //    count.foreach(println(_))
    // 转化为数组
    count.collect().foreach(println(_))
  }
}
