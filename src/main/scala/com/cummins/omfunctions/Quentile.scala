package com.cummins.omfunctions

import com.cummins.utils.SuperJob
import org.apache.spark.rdd.RDD

/**
 * 2021/6/24
 * create by rr398
 * (Spark Quentile)
 * READ:[table names]
 * SAVE:[table names]
 */
object Quentile extends SuperJob {

  override def run(): Unit = {

    val ints: Seq[Double] = Seq(7, 15, 36, 39, 40, 41)
    val data: RDD[Double] = spark.sparkContext.makeRDD(ints)

    val d1: Double = computePercentile(data, 25)
    println(d1)

  }

  def computePercentile(data: RDD[Double], tile: Double): Double = {

    val r: RDD[Double] = data.sortBy((x: Double) => x)
    val n: Long = r.count()
    if (n == 1) r.first()
    else {
      val q: Double = (tile / 100d) * (n + 1d)
      val c: Long = math.floor(q).toLong //返回小于等于x的最大整数
      val d: Double = q - c
      if (c <= 0) r.first()
      else {
        val index: RDD[(Long, Double)] = r.zipWithIndex().map((_: (Double, Long)).swap)
        val last: Long = n
        if (c >= n) {
          println("run here")
          index.lookup(last - 1).head
        } else {
          index.lookup(c - 1).head + d * (index.lookup(c).head - index.lookup(c - 1).head)
        }
      }
    }
  }

}
