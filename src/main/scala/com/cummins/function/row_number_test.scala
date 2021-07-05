package com.cummins.function

import com.cummins.utils.SuperJob
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._


/**
 * 2020/11/2
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object row_number_test extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\MileageAndSpeed.json")
    df.show()

    val spec: WindowSpec = Window.partitionBy("ESN").orderBy(asc_nulls_last("report_date"))

    val grouped: Dataset[Row] = df.select("ESN", "report_date", "percent_torque")
      .groupBy("ESN", "report_date").agg(
      collect_list($"percent_torque") as "percent_torque"
    )

    grouped.show()

    val result: DataFrame = grouped.withColumn("rank", row_number().over(spec))
      .where($"rank" === 1)
      .drop("rank")
    result.show(100, false)
    result.printSchema()
  }
}
