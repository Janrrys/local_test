package com.cummins.basicgrammar

/**
 * 2021/2/23
 * create by rr398
 * (scala 模式匹配)
 * READ:[table names]
 * SAVE:[table names]
 */
object Scala_match {
  def main(args: Array[String]): Unit = {
    println(matchTest(4))
  }

  def matchTest(x: Int): String = x match {
    case 1 => "one"
    case 2 => "two"
    case 3 => "many"
    case _ => "other"
  }
}
