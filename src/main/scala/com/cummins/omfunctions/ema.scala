package com.cummins.omfunctions

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object ema extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {

    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\chazhi.json")
      .withColumn("CALC_DATE", $"CALC_DATE".cast(DateType))
      .withColumn("calc_date_index", unix_timestamp('CALC_DATE).cast("timestamp"))
    df.show

    df.printSchema()

    df
      .select(min($"calc_date_index"), max($"calc_date_index"))
      .show()

    val (minp, maxp) = df
      .select(min($"calc_date_index").cast("bigint"), max($"calc_date_index".cast("bigint")))
      .as[(Long, Long)]
      .first

    val step: Long = 60 * 60 * 24 * 7

    val reference = spark
      .range(minp, maxp, step)
      .select($"id".cast("timestamp").alias("calc_date_index"))

    reference.orderBy("calc_date_index").show()

    reference.printSchema()

    //df.join(reference, Seq("calc_date_index"), "outer").na.fill(0, Seq("COUNT_NBR")).show
    df
      .select('CODE, 'PROGRAM_GROUP_NAME)
      .distinct()
      .crossJoin(reference)
      .join(df, Seq("CODE", "PROGRAM_GROUP_NAME", "calc_date_index"), "outer")
      .na.fill(0)
      .orderBy("CODE", "calc_date_index")
      .show(1000)

    println((minp, maxp))



    //    def exponentialMovingAverage(df: DataFrame, partitionColumn: String, orderColumn: String, column: String, windowSize: Int): DataFrame = {
    //      val window = Window.partitionBy(partitionColumn)
    //      val exponentialMovingAveragePrefix = "_EMA_"
    //
    //      val emaUDF = udf((rowNumber: Int, columnPartitionValues: Seq[Double]) => {
    //        val alpha = 2.0 / (windowSize + 1)
    //
    //        val adjustedWeights = (0 until rowNumber + 1).foldLeft(new Array[Double](rowNumber + 1)) { (accumulator, index) =>
    //          val d: Double = 1 - alpha
    //          val i: Int = rowNumber - index
    //          accumulator(index) = pow(1 - alpha, rowNumber - index); accumulator
    //        }
    //        (adjustedWeights, columnPartitionValues.slice(0, rowNumber + 1)).zipped.map(_ * _).sum / adjustedWeights.sum
    //      })
    //
    //
    //      df.withColumn("row_nr", row_number().over(window.orderBy(orderColumn)) - lit(1))
    //        .withColumn(s"$column$exponentialMovingAveragePrefix$windowSize", emaUDF(col("row_nr"), collect_list(column).over(window)))
    //        .drop("row_nr")
    //    }

  }
}
