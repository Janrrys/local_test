package com.cummins.test

object CollectionsTest {
  def main(args: Array[String]): Unit = {
    val ints: Seq[String] = Seq("1", "2", "3", "4", "5", "6")

    ints.foreach(println)

    println(ints.mkString(","))

    println(ints)
    test("1", "2")


  }

  def test(strings: String*): Unit = {
    println(strings.size)
  }
}
