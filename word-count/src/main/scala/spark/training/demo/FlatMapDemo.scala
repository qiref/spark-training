package spark.training.demo

/**
  * Author: YaoQi
  * Date: 2018/9/25 15:21
  * Description: 测试flatMap
  */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {

    val list = List("map", "flatMap", ("a", "b"))
    list.foreach(word => println(word))
    val chars = list.flatMap(word => word.toString)
    list.foreach(println(_))

    SplitLine.print()

    val book = List("hadoop","Hive","spark")
    val listBook = book.flatMap(s=> s.toList)
    listBook.foreach(println(_))
  }
}
