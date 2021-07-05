package com.cummins.spark


import com.cummins.utils.SuperJob
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._


/**
 * 2021/2/24
 * create by rr398
 * (sparksql dataframe test)
 * READ:[table names]
 * SAVE:[table names]
 */
object dataframe_test extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val efpa_df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\FC_NonimFC2.json")
//      .withColumn("OCC_DATE", to_utc_timestamp($"OCC_DATE", "GMT+8"))

    efpa_df.show(100, false)
    efpa_df.printSchema()

    val spec: WindowSpec = Window.partitionBy("ESN").orderBy(desc_nulls_last("OCC_DATE"), desc_nulls_last("FAULT_CODE_LIST"))

    val result: DataFrame = efpa_df
//      .withColumn("DATE", to_date(from_unixtime(unix_timestamp($"OCC_DATE", "yyyyMMdd")), "yyyy-MM-dd"))
//      .withColumn("OCCURRENCE_DATE", expr("DATE"))
      .withColumn("rank", row_number().over(spec))
//      .where($"rank" === 1)
//      .drop("rank")

    result.show(100, false)


  }

}
