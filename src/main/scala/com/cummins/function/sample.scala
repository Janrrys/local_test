package com.cummins.function

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

/**
 * 2020/11/24
 * create by rr398
 * READ:[table names]
 * SAVE:[table names]
 */
object sample extends SuperJob {
  override def run(): Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\MileageAndSpeed.json")
    df.show()
    val sample_df: Dataset[_] = getRandom(df, 6)
    sample_df.show()
    println(sample_df.count())

  }

  def getRandom(df: Dataset[_], n: Int) = {
    val count: Long = df.count()
    println(count)
    val percentage: Double = n.toDouble / count
    df.sample(percentage)
  }
}
