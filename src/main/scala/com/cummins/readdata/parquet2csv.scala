package com.cummins.readdata

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 2020/11/16
 * create by RR398
 * READ:[table names]
 * SAVE:[table names]
 */
object parquet2csv extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val df: DataFrame = spark
      .read
      .parquet("C:\\Users\\rr398\\Desktop\\Work\\Issue_Driver\\Data\\iss_dri_mi_parquet2csv\\part-00000-fc4fd2fc-5cf1-40ca-9d83-a1fbdf55deb9.c000.snappy.parquet")
      .withColumn("OCCURRENCE_DATE_TIME_LIST", concat_ws(",", $"OCCURRENCE_DATE_TIME_LIST"))
      .withColumn("FAULT_CODE_LIST", concat_ws(",", $"FAULT_CODE_LIST"))
      .withColumn("FAULT_CODE_DESCRIPTION_LIST", concat_ws(",", $"FAULT_CODE_DESCRIPTION_LIST"))
      .withColumn("DERATE_FLAG_LIST", concat_ws(",", $"DERATE_FLAG_LIST"))
      .withColumn("SHUTDOWN_FLAG_LIST", concat_ws(",", $"SHUTDOWN_FLAG_LIST"))
      .withColumn("FAULT_CODE_CATEGORY_LIST", concat_ws(",", $"FAULT_CODE_CATEGORY_LIST"))
      .withColumn("NONIM_OCCURRENCE_DATE_TIME_LIST", concat_ws(",", $"NONIM_OCCURRENCE_DATE_TIME_LIST"))
      .withColumn("NONIM_FAULT_CODE_LIST", concat_ws(",", $"NONIM_FAULT_CODE_LIST"))
      .withColumn("NONIM_FAULT_CODE_DESCRIPTION_LIST", concat_ws(",", $"NONIM_FAULT_CODE_DESCRIPTION_LIST"))
      .withColumn("NONIM_DERATE_FLAG_LIST", concat_ws(",", $"NONIM_DERATE_FLAG_LIST"))
      .withColumn("NONIM_SHUTDOWN_FLAG_LIST", concat_ws(",", $"NONIM_SHUTDOWN_FLAG_LIST"))
      .withColumn("NONIM_FAULT_CODE_CATEGORY_LIST", concat_ws(",", $"NONIM_FAULT_CODE_CATEGORY_LIST"))
      .withColumn("REL_CMP_ENGINE_MILES_LIST", concat_ws(",", $"REL_CMP_ENGINE_MILES_LIST"))
      .withColumn("REL_CMP_CLAIM_NUM_LIST", concat_ws(",", $"REL_CMP_CLAIM_NUM_LIST"))
      .withColumn("REL_CMP_FAIL_CODE_LIST", concat_ws(",", $"REL_CMP_FAIL_CODE_LIST"))
      .withColumn("REL_CMP_FAILURE_PART_LIST", concat_ws(",", $"REL_CMP_FAILURE_PART_LIST"))
      .withColumn("REL_CMP_CLAIM_DATE_LIST", concat_ws(",", $"REL_CMP_CLAIM_DATE_LIST"))
      .withColumn("REL_CMP_PART_NUM_LIST", concat_ws(",", $"REL_CMP_PART_NUM_LIST"))
      .withColumn("REL_CMP_BASE_OR_ATS_LIST", concat_ws(",", $"REL_CMP_BASE_OR_ATS_LIST"))
      .withColumn("REL_CMP_PAID_OPEN_LIST", concat_ws(",", $"REL_CMP_PAID_OPEN_LIST"))
      .withColumn("REL_CMP_DATA_PROVIDER_LIST", concat_ws(",", $"REL_CMP_DATA_PROVIDER_LIST"))
      .withColumn("REL_CMP_FAIL_DATE_LIST", concat_ws(",", $"REL_CMP_FAIL_DATE_LIST"))
      .withColumn("REL_CMP_CAUSE_LIST", concat_ws(",", $"REL_CMP_CAUSE_LIST"))
      .withColumn("REL_CMP_COMPLAINT_LIST", concat_ws(",", $"REL_CMP_COMPLAINT_LIST"))
      .withColumn("REL_CMP_CORRECTION_LIST", concat_ws(",", $"REL_CMP_CORRECTION_LIST"))
      .withColumn("EFPA_CMP_DAILY_ECMCODE_LIST", concat_ws(",", $"EFPA_CMP_DAILY_ECMCODE_LIST"))
      .withColumn("TEL_FC_CMP_DAILY_LONGITUDE_LIST", concat_ws(",", $"TEL_FC_CMP_DAILY_LONGITUDE_LIST"))
      .withColumn("TEL_FC_CMP_DAILY_LATITUDE_LIST", concat_ws(",", $"TEL_FC_CMP_DAILY_LATITUDE_LIST"))
      .withColumn("TEL_FC_CMP_DAILY_NONIM_LATITUDE_LIST", concat_ws(",", $"TEL_FC_CMP_DAILY_NONIM_LATITUDE_LIST"))
      .withColumn("TEL_FC_CMP_DAILY_NONIM_LONGITUDE_LIST", concat_ws(",", $"TEL_FC_CMP_DAILY_NONIM_LONGITUDE_LIST"))
      .withColumn("REL_CMP_DEALER_NAME_LIST", concat_ws(",", $"REL_CMP_DEALER_NAME_LIST"))
      .drop("EFPA_CMP_DAILY_BITREGENTYPE_COUNT", "EFPA_CMP_DAILY_CCPO_COUNT")

    df.show(100, false)
    df.printSchema()


    df.coalesce(1).write.option("header", "true").csv("C:\\Users\\rr398\\Desktop\\Work\\Issue_Driver\\Data\\")
  }
}
