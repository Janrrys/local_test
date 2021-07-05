package com.cummins.basicgrammar

import scala.util.matching.Regex

/**
 * 2021/2/23
 * create by rr398
 * (scala 正则表达式)
 * READ:[table names]
 * SAVE:[table names]
 */
object Scala_regex {
  def main(args: Array[String]): Unit = {
    val pattern1: Regex = "java".r
    val pattern2 = new Regex("(S|s)cala") //将以字符串形式提供的正则表达式编译为*可以与输入匹配的模式
    val pattern3 = new Regex("abl[ae]\\d+")
    val str = "Scala is Scalable and cool"
    val str2 = "ablaw is abla1 and cool"

    println(pattern1.findFirstIn(str))
    println(pattern2.replaceFirstIn(str, "Java"))
    println(pattern3.findAllIn(str2).mkString(","))

  }
}
