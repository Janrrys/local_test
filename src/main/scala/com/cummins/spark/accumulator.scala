package com.cummins.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 2021/6/18
 * create by rr398
 * (spark 累加器)
 * READ:[table names]
 * SAVE:[table names]
 */
object accumulator {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Application")
    val sc = new SparkContext(conf)

    //    var sum = 0
    //    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3))
    //    rdd.map((item: Int) => {
    //      sum = sum + item
    //    }).collect()
    //    println(sum)


    /**
     * 累加器
     */

    //    val sum: LongAccumulator = sc.longAccumulator
    //    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3))
    //    rdd.map((item: Int) => {
    //      sum.add(item)
    //    }).collect()
    //    println("sum = " + sum.value)
    //    sc.stop()


    /**
     * 自定义累加器，应用场景：筛选数据
     */

    val acc = new MyBlackAccumulator
    sc.register(acc, "accumulator")
    val rdd: RDD[String] = sc.makeRDD(Array("abc", "bcd", "efg"))
    rdd.map((s: String) => {
      acc.add(s)
    }).collect()
    println(acc.value)

  }

  class MyBlackAccumulator extends AccumulatorV2[String, java.util.HashSet[String]] {

    var blackList = new util.HashSet[String]()

    override def isZero: Boolean = {
      blackList.isEmpty
    }

    override def copy(): AccumulatorV2[String, util.HashSet[String]] = {
      val acc = new MyBlackAccumulator
      acc
    }

    override def reset(): Unit = {
      blackList.clear()
    }

    //包含b的加入黑名单，筛选逻辑写在add中
    override def add(v: String): Unit = {
      if (v.contains("b")) {
        blackList.add(v)
      }
    }

    override def merge(other: AccumulatorV2[String, util.HashSet[String]]): Unit = {
      blackList.addAll(other.value)
    }

    override def value: util.HashSet[String] = blackList
  }

}
