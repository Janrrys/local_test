package com.cummins.teach

import java.util
import scala.collection.immutable

/**
 * 2020/10/26
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object BinArray {

  var minValue = 1
  var field = "CCPO"
  val strings = new Array[String](100)

  def main(args: Array[String]): Unit = {

    val map = new util.HashMap[Int, String]()
    map.put(1, "Soot")
    map.put(2, "Delta P Limit")
    map.put(3, "Override")
    map.put(4, "Timed")
    map.put(5, "Ineff Regen")
    map.put(6, "Forced Regen")
    map.put(7, "DC")
    map.put(8, "Unknown")
    map.put(9, "Reactive")
    map.put(10, "Auto NM")
    map.put(11, "DeSOx 12  DOC")

    val str = "11101000101"

    val array: Array[Char] = str.toCharArray.reverse

    val ints: immutable.IndexedSeq[Int] = for {index <- array.indices if array(index) == '1'} yield index + 1

    println("ints.toBuffer:" + ints.toBuffer)

    val strings: immutable.IndexedSeq[String] = ints.map((x: Int) => {
      map.get(x)
    })
    println("strings:" + strings)

    val chars: Array[Char] = str.toCharArray.reverse
    val seq: immutable.IndexedSeq[Int] = for {index <- chars.indices if chars(index) == '1'} yield index + 1
    seq.foreach((x: Int) => {
      println(x)
      field = field + x
    })
    println(field) //CCPO13791011
  }
}
