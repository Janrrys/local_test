package com.cummins.Section5_functions

/**
 * 2021/2/26
 * create by rr398
 * (scala 函数，高阶函数，闭包)
 * READ:[table names]
 * SAVE:[table names]
 */
object Higher_order_function {
  def main(args: Array[String]): Unit = {

    //函数
    val increase1: Int => Int = (x: Int) => x + 1

    println(increase1(10)) //11

    //等同于increase1
    def increase2(x: Int): Int = x + 1

    println(increase2(10)) //11

    //匿名函数
    println(Array(2, 3, 4).map((x: Int) => x + 1).mkString(",")) //3,4,5


    //数组map使用函数
    println(Array(1, 2, 3).map(increase2).mkString(",")) //2,3,4
    println(Array(1, 2, 3).map((_: Int) + 1).mkString(",")) //2,3,4
    println(Array(1, 2, 3).map((x: Int) => {
      x + 1
    }).mkString(",")) //2,3,4

    /**
     * 高阶函数
     */

    def convertIntToString(f: Int => String) = f(1)

    println(convertIntToString((x: Int) => x + "s"))

    def multiplyBy(factor: Double) = { (x: Double) => factor * x }

    val d: Double => Double = multiplyBy(10)
    println(d(1.2))

    /**
     * 闭包
     */

    var more = 1
    val fun: Int => Int = (x: Int) => x + more

    println(fun(10))

    //下列函数也是一种闭包，因为在运行时其值才得以确定
    def multiplyBy1(factor: Double) = (x: Double) => factor * x
  }
}
