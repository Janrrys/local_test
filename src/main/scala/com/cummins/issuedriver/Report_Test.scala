package com.cummins.issuedriver

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import com.cummins.utils.Utils._

/**
 * 2020/11/3
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object Report_Test extends SuperJob {

  import spark.implicits._

  val df: DataFrame = spark
    .read
    .option("header", value = true)
    .option("sep", ";")
    .csv("C:\\Users\\rr398\\IdeaProjects\\local_test\\String2list.csv")

  df.show(100, false)

  override def run(): Unit = {
    val df1: DataFrame = df.withColumn("codlist", distinctString($"codlist"))
    df1.printSchema()
    df1.show()

  }


}