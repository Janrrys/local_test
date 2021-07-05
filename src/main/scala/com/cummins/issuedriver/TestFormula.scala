package com.cummins.issuedriver

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable


/**
  * 2020/10/29
  * create by rr398 
  * READ:[table names]
  * SAVE:[table names]
  */
object TestFormula extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\MileageAndSpeed.json")
    val df1: DataFrame = df
      .withColumn("percent_torque", percent_torque($"percent_torque"))
      .withColumn("speed", speed($"engine_speed".cast("Int")))

    val grouped: DataFrame = df1.groupBy("ESN", "report_date", "percent_torque", "speed").count()
    val result: DataFrame = grouped
      .withColumn("result", array($"percent_torque", $"speed", $"count"))
      .drop("percent_torque", "speed", "count")
      .groupBy("ESN", "report_date")
      .agg(
        collect_list("result") as "result"
      )
      .withColumn("explode", explode($"result"))
//      .withColumn("count", size($"explode"))
    //      .withColumn("first", $"explode" (0))
    //      .withColumn("second", $"explode" (1))
    //      .withColumn("third", $"explode" (2))

    result.show(100, false)
    result.printSchema()

  }

  def pickUp = udf(
    (a: mutable.WrappedArray[Long], b: mutable.WrappedArray[Long]) => {
      a.toList ::: b.toList
    }
  )

  def pickUpLists: UserDefinedFunction = udf {
    a: mutable.WrappedArray[mutable.WrappedArray[String]] => {
      if ((a != null) && a.nonEmpty) {
        a.filter(_ != null).flatMap((_: mutable.WrappedArray[String]).toList)
      } else null
    }
  }


  val percent_torque: UserDefinedFunction = udf(
    (n: Int) => {
      if (n >= 0 && n <= 100) {
        if (n == 0) {
          10
        } else {
          n % 10 match {
            case 0 => (n / 10) * 10
            case _ => ((n / 10) * 10) + 10
          }
        }
      } else -1
    }
  )

  val speed: UserDefinedFunction = udf(
    (n: Int) => {
      if (n >= 0 && n < 3000) {
        if (n > 600) {
          600 + ((n - 600) / 200 + 1) * 200
        } else 600
      } else -1

    }
  )

}
