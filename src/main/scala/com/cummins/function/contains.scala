package com.cummins.function

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object contains {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WithColunm").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val df1: Seq[String] = Seq(
      "进气节流阀", "Intake Air Throttle Actuator", "IAT",
      "喷油器", "Injector",
      "高压油轨", "高压共轨", "Fuel Lines", "Common Rail",
      "喷油器", "Injector",
      "发动机电控模块", "电控模块", "电控单元", "控制模块", "Engine Control Module", "ECM", "ECU",
      "后处理柴油氧化催化器", "氧化催化", "Diesel Oxidation Catalyst", "DOC",
      "后处理柴油机颗粒物滤清器", "颗粒捕捉器", "颗粒捕集器", "Diesel Particulate Filter", "DPF",
      "后处理选择性催化还原器", "催化转化器", "排期处理器", "后处理总成", "排气后处理器", "EGP",
      "后处理温度传感器", "温度传感器", "Aftertreatment Temperature Sensors",
      "后处理柴油机颗粒物滤清器压差传感器", "压差传感器", "Diesel Particulate Filter Differential Pressure Sensor", "Delta P Sensor", "DP sensor",
      "尿素喷射泵", "尿素泵", "后处理给料", "Dosing Unit", "Supply Module", "Doser", "Dosing Module",
      "后处理氮氧化物传感器", "氮氧", "NOx Sensor", "Nox")

    val df2: Seq[String] = Seq("进气节流阀", "模块", "传感器")

    val dataFrame1: DataFrame = sc.makeRDD(df1).toDF("FAILURE_PART")
    dataFrame1.show()
    val dataFrame2: DataFrame = sc.makeRDD(df2).toDF("FAILURE")
    val resultdf: DataFrame = dataFrame1.join(dataFrame2, upper(dataFrame1("FAILURE_PART")).contains(upper(dataFrame2("FAILURE"))))
    resultdf.show(100, truncate = false)

  }

}
