package com.cummins.issuedriver

import java.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


/**
  * 2020/10/21
  * create by rr398 
  * READ:[table names]
  * SAVE:[table names]
  */
object TestEvenlySplit {

  // 定义一个map,用来存储每个区间的范围,key:区间下限 -> value:区间上限
  private val intervalMap = new util.HashMap[Long, Long]()
  // 存储每个区间范围及所对应的区间条数:[(区间下限,区间上限),区间内数据条数]
  private val intervalCounter = new util.HashMap[(Long, Long), Long]()
  // 分成20份
  val numberOfCopies = 20

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\Mileage.json").cache()

    // 获取最大里程与最小里程,df3中,只有一行,两列数据：最大值，最小值
    val df3: DataFrame = df.select(max("Mileage"), min("Mileage"))
    df3.show()

    // 获取df3中的数据转化为本地数据类型
    val max_and_min: Array[Row] = df3.collect()
    val maxm: Long = max_and_min(0).getLong(0)
    val minm: Long = max_and_min(0).getLong(1)

    // 确定单位长度
    val unit: Long = (maxm - minm) / 20

    // 定义遍历指针,并初始化指向区间初始最小值
    var start: Long = minm
    var end: Long = minm

    // 确定区间范围并存入intervalMap,start每次增加一个unit,从而每次指向下一个区间的开始
    // end比start 多走一个unit,从而指向区间的结束
    for (i <- 0 until numberOfCopies) {
      start = minm + (i * unit)
      end = minm + ((i + 1) * unit)
      intervalMap.put(start, end)
    }

    // 将intervalMap 由Java HashMap 转化为 Scala数据类型并排序
    val intervalList: List[(Long, Long)] = intervalMap.asScala.toList.sortBy((_: (Long, Long))._1)
    println(intervalList)

    // 统计每个区间内的数据条数,并在此区间内的数据条数 > 0 时,将区间信息及条数存入intervalCounter
    intervalList.foreach((tuple: (Long, Long)) => {
      val intervalCount: Long = df.where(($"Mileage" > tuple._1) && ($"Mileage" <= tuple._2)).count()
      if (intervalCount > 0) {
        intervalCounter.put(tuple, intervalCount)
        println(s"$tuple 区间内 -> $intervalCount")
      }
    })

    println(intervalCounter.asScala)
//    sample(df)

  }

//  /**
//    *
//    * @param df
//    */
//  def sample(df: DataFrame) = {
//
//    intervalCounter.toList.foreach((x: ((Long, Long), Long)) => {
//      //todo:利用已经构建好的区间以及区间内数据信息，对需要进行抽样的数据集进行操作
//      //df.where()
//      println(x._2)
//    })
//
//  }

}
