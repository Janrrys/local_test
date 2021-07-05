package com.cummins.omfunctions

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * 2021/6/17
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object DataTypes {

  def main(args: Array[String]): Unit = {

    //创建密集向量，索引从0开始
    val vd: linalg.Vector = Vectors.dense(2, 0, 6)
    //密集向量建立标记点
    val point = LabeledPoint(1, vd)
    println(point.features)
    println(point.label)
  }
}
