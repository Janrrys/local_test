package com.cummins.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object maxList {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("maxList").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df1: Seq[String] = Seq("20200921", "20200922")
    val dataFrame: DataFrame = sc.makeRDD(df1).toDF("Date")
    val df2: DataFrame = dataFrame.withColumn("Date", to_date(from_unixtime(unix_timestamp($"Date", "yyyyMMdd")), "yyyy-MM-dd"))
    df2.show()
    df2.printSchema()

  }

}
