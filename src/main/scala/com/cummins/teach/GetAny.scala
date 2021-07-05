package com.cummins.teach

/**
 * 2020/10/25
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object GetAny {

  case class Student(name: String, age: Int)

  val jiaying: Student = Student("jiaying", 18)
  val seq: Seq[Any] = Seq(1, 2, 3, "s", 'c', jiaying)

  def main(args: Array[String]): Unit = {
    val d: Double = getStudent(5).age * 0.7
    println(d.toInt /*直接取整数位*/)

  }

  def getInt(index: Int): Int = {
    seq(index).asInstanceOf[Int]
  }

  def getLong(index: Int): Long = {
    seq(index).asInstanceOf[Long]
  }

  def getStudent(index: Int): Student = {
    seq(index).asInstanceOf[Student]
  }
}
