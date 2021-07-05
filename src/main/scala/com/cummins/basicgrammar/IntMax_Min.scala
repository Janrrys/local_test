package com.cummins.basicgrammar

/**
 * 2021/3/15
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object IntMax_Min {
  def main(args: Array[String]): Unit = {
    val max: Int = Integer.MAX_VALUE
    val min: Int = Integer.MIN_VALUE

    println(2147483647.toBinaryString)
    System.out.println("int的最大值： " + max)
    System.out.println("int的最大值+1： " + (max + 1))
    System.out.println("int的最小值： " + min)
    System.out.println("int的最小值－1： " + (min - 1))
  }

}
