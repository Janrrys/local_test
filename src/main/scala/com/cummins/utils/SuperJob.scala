package com.cummins.utils

import com.cummins.libraries.Util
import org.apache.spark.sql.SparkSession

/**
  * 2020/10/22
  * create by rr398 
  * READ:[table names]
  * SAVE:[table names]
  */
trait SuperJob {
  val spark: SparkSession = Util.getSpark()

  def main(args: Array[String]): Unit = {
    run()
  }

  def run()
}
