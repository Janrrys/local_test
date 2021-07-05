package com.cummins.basicgrammar

import Array._

/**
 * 2021/2/22
 * create by rr398
 * (scala array)
 * READ:[table names]
 * SAVE:[table names]
 */
object scala_array {
  def main(args: Array[String]): Unit = {
    val array1 = Array(1.1, 1.2, 1.3, 1.4)
    val array2 = Array(2.1, 2.2, 2.3, 2.4)

    //输出所有元素
    for (elem <- array1) {
      println(elem)
    }

    //计算数组所有元素总和
    var total = 0.0
    for (elem <- array1.indices) {
      total += array1(elem)
    }
    println("总和为" + total)

    // 查找数据最大值
    var max: Double = array1(0)
    for (elem <- array1.indices) {
      if (elem > max) max = array1(elem)
    }
    println("最大值" + max)

    //合并数组
    val array_concat: Array[Double] = concat(array1, array2)
    for (elem <- array_concat) {
      println(elem)
    }

    //创建区间数组
    val mylist1: Array[Int] = range(10, 20, 5)
    val mylist2: Array[Int] = range(10, 20)

    for (elem <- mylist1) {
      println(" " + elem)
    }
    println()
    for (elem <- mylist2) {
      println(" " + elem)
    }
  }
}
