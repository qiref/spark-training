package com.scala.demo

/**
  * Author: Yao Qi
  * Date: 2018/10/24 20:45
  * Description:打印一条线
  */
object SplitLine {

  def print(): Unit = {
    println("")
    println("-----------------------------------------------------")
    println("")
  }

  def filter(xs: List[Int], p: Int => Boolean): List[Int] =
    if (xs.isEmpty) xs
    else if (p(xs.head)) xs.head :: filter(xs.tail, p)
    else filter(xs.tail, p)

  def modN(n: Int)(x: Int): Boolean = (x % n) == 0

  def main(args: Array[String]): Unit = {
    val nums = List(1, 2, 3, 4, 5, 6, 7, 8)
//    println(modN(3))
//    val mod = modN(2)
//    println(mod(32))
//    println(filter(nums, modN(2)))
//    println(filter(nums, modN(3)))
  }
}
