package com.cummins.basicgrammar

/**
 * 2021/2/23
 * create by rr398
 * (scala list)
 * READ:[table names]
 * SAVE:[table names]
 */
object Scala_list {
  def main(args: Array[String]): Unit = {
    //字符串列表
    val list1 = List("Runoob", "Google", "Baidu")
    //整型列表
    val list2 = List(1, 2, 3, 4)
    //空列表
    val empty1 = List()
    val empty2: Nil.type = Nil
    //二维列表
    val dim_list = List(
      List(1, 0, 0),
      List(1, 1, 0),
      List(1, 1, 1)
    )
    //list fill
    val list_fill: List[String] = List.fill(3)("jiaying")
    println("list_fill" + list_fill)

    //list tabulate
    //通过给定函数创建5个元素
    val squares: List[Int] = List.tabulate(6)((n: Int) => n * n)
    println("一维 ：" + squares)
    val mul: List[List[Int]] = List.tabulate(4, 5)((_: Int) * (_: Int))
    println("多维" + mul)

  }
}
