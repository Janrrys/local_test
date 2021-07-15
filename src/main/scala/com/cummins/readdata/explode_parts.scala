package com.cummins.readdata

/**
 * 读取csv数据并将结果写入sqlserver
 */

import java.util.Properties
import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object explode_parts extends SuperJob {

  import spark.implicits._
  val properties = new Properties
  properties.load(this.getClass.getClassLoader.getResourceAsStream("db.properties"))

  override def run(): Unit = {

    val df: DataFrame = spark
      .read
      .option("header", value = true)
      .option("sep", ",")
      .csv("C:\\Users\\rr398\\IdeaProjects\\local_test\\issue driver_parts.csv")
    df.show(100, false)

    val result = df
      .withColumn("FAILURE_PART", split($"FAILURE_PART", ","))
      .withColumn("FAILURE_PART", explode($"FAILURE_PART"))

    result.printSchema()
    result.show(100, false)

    result.write.mode("overwrite").jdbc(properties.getProperty("url"), s"[QA7S].[DIM_PERMISSION]", properties)

  }
}
