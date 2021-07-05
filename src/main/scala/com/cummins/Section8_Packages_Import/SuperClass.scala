package com.cummins.Section8_Packages_Import

/**
 * protected 成员 protected成员只能被该类及其子类访问
 */
class SuperClass {
  protected def f(): Unit = println(".....")

}

class SubClass extends SuperClass {
  f()
}

//class OtherClass{
//  f()
//}

