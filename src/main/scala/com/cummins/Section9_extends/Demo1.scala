package com.cummins.Section9_extends

/**
 * 2021/3/15
 * create by rr398
 * (多态：子类可以出现在父类出现的任何地方)
 * READ:[table names]
 * SAVE:[table names]
 */

class Person(name: String, age: Int) {
  def walk(): Unit = println("我在走路")
}

class Student(name: String, age: Int, var studentNo: String) extends Person(name, age) {
  override def walk(): Unit = {
    //super.walk()
    println("我在背着书包走")
  }
}

class Teacher(name: String, age: Int) extends Person(name, age) {
  override def walk(): Unit = {
    //super.walk()
    println("我在拿着教案走")
  }
}

object Demo1 {
  def main(args: Array[String]): Unit = {
    val songTeacher = new Teacher("Song", 45)
    val mingStudent = new Student("Ming", 13, "12345")

    userWalk(songTeacher)


    userWalk(mingStudent)

  }


  def userWalk(person: Person): Unit = {
    person.walk()
  }

}
