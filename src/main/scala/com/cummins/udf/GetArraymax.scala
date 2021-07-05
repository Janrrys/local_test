package com.cummins.udf

import com.cummins.utils.SuperJob
import com.cummins.utils.Utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * 2020/11/9
 * create by RR398
 * READ:[table names]
 * SAVE:[table names]
 */
object GetArraymax extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\FcAndFp.json")
    df.show()

    val grouped: DataFrame = df.groupBy("ESN", "OCC_DATE")
      .agg(
        collect_list($"FAULT_CODE") as "FAULT_CODE1"
      )
    grouped.show(100, truncate = false)
    grouped.printSchema()

    val agg_df: DataFrame = df.groupBy("ESN", "OCC_DATE").agg(sum($"FAULT_CODE") as "FAULT_CODE2")
    agg_df.show()

    val joined_df: DataFrame = grouped.join(agg_df, Seq("ESN"), "left")
    joined_df.show()
    joined_df.printSchema()

    val result_df: DataFrame = joined_df
      .withColumn("FAULT_CODE", split(concat_ws(",", $"FAULT_CODE1", $"FAULT_CODE2"), ","))
      .withColumn("MAX_FAULT_CODE", arrayMax($"FAULT_CODE"))

    result_df.show(false)
    result_df.printSchema()

  }
}
