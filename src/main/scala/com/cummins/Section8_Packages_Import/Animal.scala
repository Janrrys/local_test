package com.cummins.Section8_Packages_Import

abstract class Animal {
  //抽象字段
  var height: Int

  //抽象方法
  def eat(): Unit

}

class Person(var height: Int) extends Animal {
  override def eat(): Unit = {
    println("eat by mouth")
  }
}

object Person extends App {
  new Person(10).eat()
}