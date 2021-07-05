package com.cummins.test

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame

/**
 * 2021/3/1
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object test_input extends SuperJob{
  override def run(): Unit = {
    val all_df: DataFrame = spark.read.option("header",true).csv("C:\\Users\\rr398\\IdeaProjects\\local_test\\all_columns.csv")
    val some_df: DataFrame = spark.read.option("header",true).csv("C:\\Users\\rr398\\IdeaProjects\\local_test\\some_columns.csv")

    some_df.join(all_df,some_df.columns.toSeq,"outer").show()

  }
}
