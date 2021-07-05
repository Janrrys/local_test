package com.cummins.Section8_Packages_Import

/**
 * 访问控制
 * private 成员 所有带该关键字修饰的成员仅能在定义它的类或对象中使用，在外部是不可见的
 */

class Student(var name: String, var age: Int) {
  private val sex: String = "女"

  //内部类
  class Course(val cname: String) {
    //可以直接访问其外部类的私有成员
    def getStudentSex(student: Student): String = student.sex
  }

}

object Student {
  private var studentNo: Int = 0;

  def uniqueStudentNo(): Int = {
    studentNo += 1
    studentNo
  }

  def apply(name: String, age: Int): Student = new Student(name, age)

  def main(args: Array[String]): Unit = {
    println(Student.uniqueStudentNo())

    val s = new Student("john", 29)
    println(s.sex)
    println(s.age)
    println(s.name)

    val s1 = Student("jenne", 20)
    val c1 = new s1.Course("scala")

  }
}

/**
 * private[this]，限定只有该类的对象才能访问，称这种成员为对象私有成员
 */

class Teacher(var name: String) {
  private[this] def printName(tName: String = ""): Unit = {
    println(tName)
  }

  def print(n: String): Unit = this.printName(n)
}

object Teacher {
  //private[this]限定的成员，即使伴生对象Teacher也不能使用
  //def printName=new Teacher("john").printName()
}


/**
 * private 定义的类及伴生类可以访问
 */

class food(var name: String) {

  private def printName(fname: String = ""): Unit = {
    println(fname)
  }

  def print(f: String): Unit = this.printName("tiramisu")

}

object food {
  def printName(): Unit = new food("tiramisu").printName()
}

object appDemo {
  def main(args: Array[String]): Unit = {
    //不能访问
    //new Teacher("john").printName()
  }

}

































