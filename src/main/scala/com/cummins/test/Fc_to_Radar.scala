package com.cummins.test

import java.text.SimpleDateFormat
import java.util.Date

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import com.cummins.utils.Utils._
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

/**
 * 2021/6/11
 * create by rr398
 * (RADAR上Fault code信息补全)
 * READ:[table names]
 * SAVE:[table names]
 */
object Fc_to_Radar extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {

    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\uv_master.json")
    df.printSchema()
    df.show(50, truncate = false)

    val result: DataFrame = df
      .withColumn("fault_code_list", removeSympol($"fault_code_list"))
      .withColumn("nonim_fault_code_list", removeSympol($"nonim_fault_code_list"))
      .withColumn("QA_Fault_Code_List", pickUpString($"fault_code_list", $"nonim_fault_code_list"))
      .withColumn("QA_Fault_Code_List", distinctArrayString($"QA_Fault_Code_List"))
      .withColumn("REL_CMP_CLAIM_NUM", splitString2Array($"rel_cmp_claim_num_list"))
      .withColumn("REL_CMP_CLAIM_NUM", explode($"REL_CMP_CLAIM_NUM"))
      .drop("fault_code_list","nonim_fault_code_list","rel_cmp_claim_num_list")

    result.printSchema()
    result.show(100, truncate = false)

  }


  val removeSympol: UserDefinedFunction = udf((col: String) => {
    if (col.nonEmpty && col != null) {

      val split: Array[String] = col.split(",")

      var result = Array[String]()

      for (elem <- split) {
        val str: String = elem.toString.replaceAll("\\*", " ")
        val fc: String = str.substring(0, 5)
        result = result :+ fc
      }
      result

    } else null
  })
}


