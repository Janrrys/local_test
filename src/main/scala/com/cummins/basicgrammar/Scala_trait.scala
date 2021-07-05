package com.cummins.basicgrammar

/**
 * 以下Trait由两个方法组成，isEqual方法没有定义方法的实现，
 * isNotEqual定义了方法的实现，子类继承特征实现未被实现的方法
 */
trait scala_trait {

  def isEqual(x: Any): Boolean

  def isNotEqual(x: Any): Boolean = !isEqual(x)
}

class Point1(xc: Int, yc: Int) extends scala_trait {
  var x: Int = xc
  var y: Int = yc

  def isEqual(obj: Any): Boolean =
    obj.isInstanceOf[Point1] &&
      obj.asInstanceOf[Point1].x == x

}

object Test {
  def main(args: Array[String]): Unit = {
    val p1 = new Point1(2, 3)
    val p2 = new Point1(2, 4)
    val p3 = new Point1(3, 3)

    println(p1.isNotEqual(p2))
    println(p1.isNotEqual(p3))
    println(p1.isNotEqual(2))
  }
}

