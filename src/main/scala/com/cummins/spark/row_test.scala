package com.cummins.spark

import com.cummins.utils.SuperJob
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * 2021/2/24
 * create by rr398
 * (spark row learn)
 * READ:[table names]
 * SAVE:[table names]
 */
object row_test extends SuperJob {

  override def run(): Unit = {

    //创建新行
    val row = Row.apply(1, "jiaying", null)
    val row1 = Row(1, 2, 3)
    println(row1) //[1,2,3]

    val row2: Row = Row.fromSeq(Seq(1, 2, 3))
    println(row2)

    //通过序数的常规访问来访问行的值
    val i: Int = row.getInt(0)
    println(i)
    val bool: Boolean = row.isNullAt(2)
    println(bool)

    //模式匹配提取Row对象中的字段
    val pairs: RDD[(Int, String)] = spark.sql("SELECT key, value FROM src").rdd.map {
      case Row(key: Int, value: String) =>
        key -> value
    }

  }
}
