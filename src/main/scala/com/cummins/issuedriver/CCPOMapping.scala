package com.cummins.issuedriver

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 2021/5/21
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object CCPOMapping extends SuperJob {

  override def run(): Unit = {

    import spark.implicits._

    val ref_ccpo_name: DataFrame = spark
      .read
      .option("header", value = true)
      .csv("C:\\Users\\rr398\\IdeaProjects\\local_test\\ref_ccpo_name.csv")

    ref_ccpo_name.show(100, truncate = false)

    val ccpo: DataFrame = spark
      .read
      .option("header", value = true)
      .csv("C:\\Users\\rr398\\IdeaProjects\\local_test\\ccpo.csv")

    ccpo.show(100, truncate = false)

    val joined_df: DataFrame = ccpo.join(ref_ccpo_name, Seq("CCPO"), "left")
      .select("CCPO", "NAME", "Count")
      .withColumn("NAME", when($"NAME".isNull, $"CCPO").otherwise($"NAME"))
    joined_df show(100, truncate = false)

  }
}
