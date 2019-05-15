package com.scala.demo

/**
  * Author: Yao Qi
  * Date: 2018/10/24 20:27
  * Description: for 循环实例
  */
object ForLoopDemo {

  def main(args: Array[String]): Unit = {
    for (x <- 1 to 7) {
      println(s"Days $x:")
    }

    SplitLine.print()

    /**
      * yield 能将循环表达式转换为集合类型，这的s表示$x中的值为String类型的
      */
    val seqCharSequence = for (x <- 1 to 7) yield {
      s"Days $x:"
    }
    println(seqCharSequence)

    SplitLine.print()

    /**
      *
      */
    val quote = "Faith,Hope,Charity"
    for {
      t <- quote.split(",")
      if t != null && t.length > 0
    } {
      println(t)
    }

  }
}
