package com.cummins.basicgrammar

/**
 * 2021/2/23
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object Scala_extractor {
  def main(args: Array[String]): Unit = {
    println("Apply 方法 : " + apply("Zara", "gmail.com"))
    println("Unapply 方法 : " + unapply("Zara@gmail.com"))
    println("Unapply 方法 : " + unapply("Zara Ali"))
  }

  //注入方法
  def apply(user: String, domain: String): String = {
    user + "@" + domain
  }

  //提取方法
  def unapply(str: String): Option[(String, String)] = {
    val parts: Array[String] = str split "@"
    if (parts.length == 2) {
      Some(parts(0), parts(1))
    } else {
      None
    }
  }

}
