package com.cummins.udf

import com.cummins.utils.SuperJob
import com.cummins.utils.Utils._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

/**
 * 2020/11/4
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object zipTest extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\zip.json")
    val grouped: DataFrame = df.groupBy("ESN").agg(
      collect_list($"FAULT_CODE") as "FAULT_CODE_LIST",
      collect_list($"NONIM_FAULT_CODE") as "NONIM_FAULT_CODE_LIST",
      collect_list($"FAILURE_PART") as "FAILURE_PART_LIST",
      collect_list($"Latitude") as "Latitude_list",
      collect_list($"Longitude") as "Longitude_list"
    )

    grouped.show(100, truncate = false)
    grouped.printSchema()

    val added: DataFrame = grouped
      .withColumn("Code_with_type", addCodeType($"FAULT_CODE_LIST", lit("fc")))
      .withColumn("nonim_Code_with_type", addCodeType($"NONIM_FAULT_CODE_LIST", lit("fc")))
      .withColumn("part_with_type", addCodeType($"FAILURE_PART_LIST", lit("fp")))
      .withColumn("Code_with_type", explode($"Code_with_type"))
      .withColumn("code", $"Code_with_type"("_1"))
      .withColumn("type", $"Code_with_type"("_2"))


    added.show(100, truncate = false)
    added.printSchema()


    val fc_code_loc: DataFrame = added
      .select("ESN", "Code_with_type", "Latitude_list", "Longitude_list", "part_with_type")
      .withColumn("code_with_loc", ZipFaultCodeWithLOC($"Code_with_type", $"Latitude_list", $"Longitude_list"))
      .withColumn("code_with_loc", explode($"code_with_loc"))
      .withColumn("Code_with_type", $"code_with_loc"("_1"))
      .withColumn("code", $"Code_with_type"("_1"))
      .withColumn("code_type", $"Code_with_type"("_2"))
      .withColumn("Latitude", $"code_with_loc"("_2"))
      .withColumn("Longitude", $"code_with_loc"("_3"))


    fc_code_loc.show(false)
    fc_code_loc.printSchema()

    val nonim_code_loc: DataFrame = added.select("ESN", "nonim_Code_with_type", "Latitude_list", "Longitude_list")
      .withColumn("code_with_loc", ZipFaultCodeWithLOC($"nonim_Code_with_type", $"Latitude_list", $"Longitude_list"))
      .withColumn("code_with_loc", explode($"code_with_loc"))
      .withColumn("Code_with_type", $"code_with_loc"("_1"))
      .withColumn("code", $"Code_with_type"("_1"))
      .withColumn("code_type", $"Code_with_type"("_2"))
      .withColumn("Latitude", $"code_with_loc"("_2"))
      .withColumn("Longitude", $"code_with_loc"("_3"))
      .select("ESN", "code", "code_type", "Latitude", "Longitude")

    nonim_code_loc.show(100, truncate = false)
    nonim_code_loc.printSchema()

    val fp_code_loc: DataFrame = added
      .select("ESN", "part_with_type")
      .withColumn("part_with_type", explode($"part_with_type"))
      .withColumn("part", $"part_with_type"("_1"))
      .withColumn("part_type", $"part_with_type"("_2"))
      .withColumn("Latitude", lit(""))
      .withColumn("Longitude", lit(""))
      .select("ESN", "part_with_type", "part", "part_type")
    println("part")
    fp_code_loc.show(100, truncate = false)

    val result2: Dataset[Row] = fc_code_loc.unionByName(nonim_code_loc).unionByName(fp_code_loc)
      .where($"code" =!= lit(""))
    result2.show(100, truncate = false)


  }
}
