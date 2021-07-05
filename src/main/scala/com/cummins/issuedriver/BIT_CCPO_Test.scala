package com.cummins.issuedriver

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.{immutable, mutable}

/**
 * 2020/10/26
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object BIT_CCPO_Test extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\Mileagedata.json")
      .withColumn("transformBITRegenType", transformBITRegenType($"mileage"))
    df.show(50, truncate = false)

  }

  def pickUpLists: UserDefinedFunction = udf {
    a: mutable.WrappedArray[mutable.WrappedArray[String]] => {
      if ((a != null) && a.nonEmpty) {
        a.filter((_: mutable.WrappedArray[String]) != null).flatMap((_: mutable.WrappedArray[String]).toList)
      } else null
    }
  }

  def arrays_zip: UserDefinedFunction = udf {
    (key: mutable.WrappedArray[String], value: mutable.WrappedArray[Int]) => {
      key.zip(value)
    }
  }


  /**
   * reduce the list and return String
   * before : ("a", "b", "c", "a", "d", "c", "a")
   * after : b $cnctor 1 $sep d $cnctor 1 $sep a $cnctor 3 $sep c $cnctor 2
   *
   * @return String
   */
  def ListReduceCount2String: UserDefinedFunction = udf((arr: mutable.WrappedArray[String], cnctor: String, sep: String) => {
    arr.map((_: String, 1))
      .groupBy((_: (String, Int))._1)
      .map((x: (String, mutable.WrappedArray[(String, Int)])) => {
        (x._1, x._2.size)
      })
      .mkString(sep)
      .replaceAll("->", cnctor)

  })

  def ListReduceCount2Map1: UserDefinedFunction = udf((arr: mutable.WrappedArray[String]) => {
    val stringToInt: Map[String, Int] = arr.map((_: String, 1))
      .groupBy((_: (String, Int))._1)
      .map((x: (String, mutable.WrappedArray[(String, Int)])) => {
        (x._1, x._2.size)
      })
    stringToInt.toList

  })

  def ListReduceCount2Map: UserDefinedFunction = udf((arr: mutable.WrappedArray[String]) => {
    val stringToInt: Map[String, Int] = arr.map((_: String, 1))
      .groupBy((_: (String, Int))._1)
      .map((x: (String, mutable.WrappedArray[(String, Int)])) => {
        x._1 -> x._2.size
      })
    stringToInt
  })


  val transformBITRegenType: UserDefinedFunction = udf(
    (BIT_Regen_Type: String) => {
      if ((BIT_Regen_Type != null) && BIT_Regen_Type.nonEmpty) {
        val binStr: String = BIT_Regen_Type.toInt.toBinaryString
        // definition: BIT_Regen_Type The data list corresponding to the position of the binary value from right to left 1
        val typemap: mutable.Map[Int, String] = mutable.Map(
          1 -> "Soot",
          2 -> "Delta P Limit",
          3 -> "Override",
          4 -> "Timed",
          5 -> "Ineff Regen",
          6 -> "Forced Regen",
          7 -> "DC",
          8 -> "Unknown",
          9 -> "Reactive",
          10 -> "Auto NM",
          11 -> "DeSOx 12  DOC"
        )
        val array: Array[Char] = binStr.toCharArray.reverse
        val ints: immutable.IndexedSeq[Int] = for {index <- array.indices if array(index) == '1'} yield index + 1
        for (elem <- ints) {
          println(elem)
        }
        val maybeStrings: immutable.IndexedSeq[String] = ints.map((x: Int) => {
          typemap(x)
        })
        maybeStrings
      } else null
    }
  )
}

