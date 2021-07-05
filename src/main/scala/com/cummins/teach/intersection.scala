package com.cummins.teach

/**
 * 2020/10/20
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object intersection {
  def main(args: Array[String]): Unit = {
    val arr1 = Array(1, 2, 3, 4, 5, 6)
    val arr2: Array[Int] = Array[Int]()

    println(intersection(arr1, arr2))


    def intersection(thisarr: Array[Int], otherarr: Array[Int]): Boolean = {
      if (thisarr == null || thisarr.isEmpty) return false
      if (otherarr == null || otherarr.isEmpty) return false
      val intersection: Array[Int] = thisarr intersect otherarr
      if (intersection.length > 0) true else false
    }
  }

  def formula(a: Int, b: Int): Double = {
    (a + b) * 365 / 3.14
  }
}
