package com.cummins.issuedriver

import com.cummins.utils.SuperJob
import com.cummins.utils.Utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 2020/11/4
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object Code_Type extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\FcAndFp.json")

    df.show()
    val agged: DataFrame = df.groupBy("ESN", "OCC_DATE").agg(
      collect_list("FAULT_CODE") as "FAULT_CODE_List",
      collect_list("FAILURE_PART") as "FAILURE_PART_List"
    )
    agged.show(100, truncate = false)


    val result: DataFrame = agged
      .withColumn("Code_Type", addCodeType($"FAULT_CODE_List", lit("FAULT_CODE")))
      .withColumn("Part_Type", addCodeType($"FAILURE_PART_List", lit("FAILURE_PART")))
      .withColumn("concat", pickUpTupleLists(array($"Code_Type", $"Part_Type")))
      .withColumn("explode", explode($"concat"))
      .withColumn("CODE", $"explode"("_1"))
      .withColumn("TYPE", $"explode"("_2"))
      .distinct()
      .drop("concat")
    result.printSchema()
    result.show(100, truncate = false)

    val COUNT: DataFrame = result.groupBy("ESN", "OCC_DATE", "CODE").count()
    COUNT.show(100, truncate = false)

  }




}
