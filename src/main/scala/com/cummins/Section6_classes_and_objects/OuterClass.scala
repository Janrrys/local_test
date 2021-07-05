package com.cummins.Section6_classes_and_objects

/**
 * 内部类
 */
class OuterClass {
  private var x: Int = 1

  class InnerClass {

    def getOuterX(a: Int): Int = x + a
  }

}

object Test {

  def main(args: Array[String]): Unit = {
    val o = new OuterClass
    val i = new o.InnerClass
    println(i.getOuterX(10))

  }
}
