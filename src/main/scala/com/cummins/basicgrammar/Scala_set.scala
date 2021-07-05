package com.cummins.basicgrammar

/**
 * 2021/2/23
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object Scala_set {
  def main(args: Array[String]): Unit = {
    val num1: Set[Int] = Set(5,6,9,20,30,45,45)
    val num2: Set[Int] = Set(50,60,9,20,35,55)
    //交集
//    println(num1.&(num2)" + num1.&(num2))
//    println(""num1.intersect(num2)" + num1.intersect(num2))

    println(num1)

  }

}
