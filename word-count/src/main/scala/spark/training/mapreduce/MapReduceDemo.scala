package spark.training.mapreduce

import org.apache.spark.sql.SparkSession

object MapReduceDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("map reduce").master("local[*]").getOrCreate()
    val textFile = spark.sparkContext.textFile("wordCount/src/main/resources/data.txt")
    textFile.map(word => word.length)
    val str = textFile.reduce((a, b) => a + b)
    println(str)
  }
}
