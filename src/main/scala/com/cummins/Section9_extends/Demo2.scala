package com.cummins.Section9_extends

/**
 * 2021/3/15
 * create by rr398
 * (匿名类，当某个类在程序中只使用一次时，可以将类定义为匿名类)
 * READ:[table names]
 * SAVE:[table names]
 */

abstract class Person2(name: String, age: Int) {
  def walk()
}


object Demo2 {
  def main(args: Array[String]): Unit = {
    val s: Person = new Person("john", 18) {
      override def walk(): Unit = {
        println("Walk like a normal Person")
      }
    }
    s.walk()
  }
}
