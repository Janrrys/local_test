package com.cummins.Section6_classes_and_objects

import java.io._

/**
 * Scala 类和对象:
 * 类是对象的抽象，对象是类的具体实例
 * 类是抽象的，不占用内存，对象是具体的，占用存储空间
 * 一个scala源文件中可以有多个类
 * Scala的类定义可以有参数，称为类参数，类参数在整个类中都能访问
 * 可以使用new 来实例化类，然后访问类中的方法和变量
 */
//class Point(xc: Int, yc: Int) {
//  var x: Int = xc
//  var y: Int = yc
//
//
//  def move(dx: Int, dy: Int): Unit = {
//    x = x + dy
//    y = y + dy
//    println("x 的坐标点: " + x);
//    println("y 的坐标点: " + y);
//  }
//
//}
//
//object Test1 {
//  def main(args: Array[String]): Unit = {
//    val point = new Point(10, 20)
//    point.move(1, 2)
//  }
//}


/**
 * scala使用extends关键字来继承一个类
 * override val xc 为重写了父类的字段
 * 继承会继承父类的所有属性和方法,Scala只允许继承一个父类。
 *
 * @param xc
 * @param yc
 */


class Point(val xc: Int, val yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
    println("x 的坐标点 : " + x);
    println("y 的坐标点 : " + y);
  }
}


class Location(override val xc: Int, override val yc: Int, val zc: Int) extends Point(xc, yc) {
  var z: Int = zc

  def move(dx: Int, dy: Int, dz: Int): Unit = {
    x = x + dx
    y = y + dy
    z = z + dz
    println("x 的坐标点 : " + x);
    println("y 的坐标点 : " + y);
    println("z 的坐标点 : " + z);
  }
}

object Test2 {
  def main(args: Array[String]): Unit = {
    val location = new Location(10, 20, 30)
    location.move(1, 2, 3)
  }
}


/**
 * scala 重写一个非抽象方法，必须用override修饰符
 */
//
//class Person {
//  var name = ""
//
//  override def toString = getClass.getName + "[name=" + name + "]"
//}
//
//
//class Employee extends Person {
//  var salary = 0.0
//
//  override def toString = super.toString + "[salary=" + salary + "]"
//}
//
//object Test3 extends App {
//  val employee = new Employee
//  employee.name = "empl"
//  employee.salary = 1000
//  println(employee)
//}

/**
 * scala 单例对象
 *
 */

//class Point(val xc: Int, val yc: Int) {
//  var x: Int = xc
//  var y: Int = yc
//
//  def move(dx: Int, dy: Int): Unit = {
//    x = x + dx
//    y = y + dy
//  }
//}
//
//object Test4 {
//  def main(args: Array[String]): Unit = {
//    val point = new Point(10, 20)
//    printPoint()
//
//    def printPoint(): Unit = {
//      println("x 的坐标点 : " + point.x);
//      println("y 的坐标点 : " + point.y);
//    }
//  }
//}










