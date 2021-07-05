package com.cummins.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

/**
 * 2021/2/5
 * create by rr398
 * (UDF function)
 * READ:[table names]
 * SAVE:[table names]
 */
object Utils {

  val pickUpTupleLists: UserDefinedFunction = udf {
    a: mutable.WrappedArray[mutable.WrappedArray[(String, String)]] => {
      if ((a != null) && a.nonEmpty) {
        a.filter((_: mutable.WrappedArray[(String, String)]) != null).flatten
      } else null
    }
  }

  val addCodeType: UserDefinedFunction = udf(
    (a: mutable.WrappedArray[String], CodeType: String) => {
      if (a != null && a.nonEmpty) {
        a.map(((_: String), CodeType))
      } else null
    }
  )

  val ZipFaultCodeWithLOC: UserDefinedFunction = udf(
    (CODE: Seq[(String, String)], Latitude: Seq[String], Longitude: Seq[String]) => {
      val values: Seq[((String, String), (String, String))] = CODE.zip(Latitude.zip(Longitude))
      values.map((value: ((String, String), (String, String))) => (value._1, value._2._1, value._2._2))
    }
  )

  val arrayMax: UserDefinedFunction = udf(
    (arr: mutable.WrappedArray[Long]) => {
      if (arr != null) {
        arr.max
      } else 0
    }
  )

  val arrayMax2: UserDefinedFunction = udf {
    (arr: mutable.WrappedArray[Integer]) => {
      arr.max
    }
  }

  val ListReduceCount2Map: UserDefinedFunction = udf((arr: mutable.WrappedArray[String]) => {
    val stringToInt: Map[String, Int] = arr.map((_: String, 1))
      .groupBy((_: (String, Int))._1)
      .map((x: (String, mutable.WrappedArray[(String, Int)])) => {
        (x._1, x._2.size)
      })
    stringToInt.toList

  })

  val pickUpLists: UserDefinedFunction = udf {
    a: mutable.WrappedArray[mutable.WrappedArray[String]] => {
      if ((a != null) && a.nonEmpty) {
        a.filter(_ != null).flatMap((_: mutable.WrappedArray[String]).toList)
      } else null
    }
  }

  def distinctString = udf(
    (a: String) => {
      if (a != null && !a.isEmpty) {
        a.split(",").toSet.mkString(",")
      } else ""
    }
  )

  def pickUpString = udf(
    (a: mutable.WrappedArray[String], b: mutable.WrappedArray[String]) => {
      if (a != null && a.nonEmpty) {
        if (b == null || b.isEmpty) {
          a.toList
        }
        else a.toList ::: b.toList
      }
      else if ((a == null || a.isEmpty) && b != null && b.nonEmpty) {
        b.toList
      }
      else null
    }
  )

  def distinctArrayString: UserDefinedFunction = udf(
    (a: mutable.WrappedArray[String]) => {
      if (a != null && a.nonEmpty) {
        a.toSet.mkString(",")
      } else ""
    }
  )

  val splitString2Array: UserDefinedFunction = udf((str:String) => {
    if (str != null && str.nonEmpty)
      str.split(",")
    else
      null
  })


}
