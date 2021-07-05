package com.cummins.spark

import com.cummins.utils.SuperJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 2021/2/24
 * create by rr398
 * (spark rdd)
 * READ:[table names]
 * SAVE:[table names]
 */
object rdd_test extends SuperJob {

  override def run(): Unit = {

        val seq: Seq[String] = Seq("EFPA_CMP_DAILY_ANROFFSETMIN_PERCENTAGE",
          "EFPA_CMP_DAILY_ANROFFSETMEAN_PERCENTAGE",
          "EFPA_CMP_DAILY_DPFDELTAREGENMEAN_DEG_C")

        val rdd: RDD[String] = spark.sparkContext.makeRDD(seq)
        val df: DataFrame = spark.emptyDataFrame
        val schema: StructType = df.schema


    //    求两个RDD中相同的部分：
    val sc: SparkContext = spark.sparkContext
    //    val x: RDD[Int] = sc.parallelize(1 to 20)
    //    val y: RDD[Int] = sc.parallelize(10 to 30)
    //    val z: RDD[Int] = x.intersection(y)
    //    val collect: Array[Int] = z.sortBy((e: Int) => e, ascending = true).collect
    //    println(collect)

    val a: RDD[String] = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b: RDD[(Int, String)] = a.map((x: String) => (x.length, x))
    val strings: Seq[String] = b.lookup(5)
    println(strings)

  }
}
