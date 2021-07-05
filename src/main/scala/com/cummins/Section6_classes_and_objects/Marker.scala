package com.cummins.Section6_classes_and_objects

import scala.swing.Color

class Marker private(val color: String) {

  println("创建" + this)

  override def toString(): String = "颜色标记：" + color
}


//伴生对象，与类名相同，可以访问类的私有属性和方法
object Marker {
  private val markers: Map[String, Marker] = Map(
    "red" -> new Marker("red"),
    "blue" -> new Marker("blue"),
    "green" -> new Marker("green")
  )

  def apply(color: String): Marker = {
    if (markers.contains(color)) markers(color) else null
  }

  def getMarker(color: String) = {
    if (markers.contains(color)) markers(color) else null
  }


  def main(args: Array[String]): Unit = {
    println(Marker("red"))
    println(Marker getMarker "blue")
  }
}