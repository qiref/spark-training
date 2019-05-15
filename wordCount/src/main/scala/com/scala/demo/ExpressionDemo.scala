package com.scala.demo

/**
  * Author: Yao Qi
  * Date: 2018/10/24 19:48
  * Description: 表达式Demo
  */
object ExpressionDemo {

  def main(args: Array[String]): Unit = {

    val day = "N"

    val kind = day match {
      case "MON" | "TUE" | "WED" | "THU" | "FRI" =>
        "weekday"
      case "SAT" | "SUN" =>
        "weekend"
      case _ =>
        println("Nothing")
        "other"
    }
    println(kind)

    val response = "ssss"
    response match {
      case s if s != null => println(s"Received'$s'")
      case s => println("Error! Received a null response")
    }

  }
}
