package com.cummins.issuedriver


import com.cummins.utils.SuperJob
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 2021/4/19
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object ShapeValue extends SuperJob {

  import spark.implicits._

  override def run(): Unit = {
    val df: DataFrame = spark
      .read
      .option("header", value = true)
      .option("sep", ";")
      .csv("C:\\Users\\rr398\\IdeaProjects\\local_test\\shap_value.csv")

    df.show(100, false)

    var newframe: DataFrame = df

    val column: Array[String] = df.columns

    column.foreach(
      (colName: String) => {
        newframe = newframe.withColumn(colName, array(lit(colName), $"$colName"))
      }
    )

    newframe.show(100, false)


    val result: DataFrame = newframe
      .withColumn("all", array(column.head, column.tail: _*))
      .select("ESN", "OCCURRENCE_DATE", "all")
      .withColumn("ESN", $"ESN"(1))
      .withColumn("OCCURRENCE_DATE", $"OCCURRENCE_DATE"(1))
      .withColumn("explode", explode($"all"))
      .withColumn("Feature", $"explode"(0))
      .withColumn("Value", $"explode"(1))
      .select("ESN", "OCCURRENCE_DATE", "Feature", "Value")
      .where($"Feature" notEqual "ESN")
      .where($"Feature" notEqual "OCCURRENCE_DATE")


    result.show(100, false)
    val where_df: Dataset[Row] = result.where($"Value" =!= 1)
    where_df.show(100, false)

  }

  def matrixToTable(
                     matrixDF: DataFrame,
                     yColumn: String,
                     xColumn: String = "xColumn",
                     valueColumnName: String = "value"
                   ) = {
    var newframe: DataFrame = matrixDF
    val column: Array[String] = matrixDF.drop(yColumn).columns
    column.foreach(
      (colName: String) => {
        newframe = newframe.withColumn(colName, array(lit(colName), $"$colName"))
      }
    )
    newframe
      .withColumn("all", array(column.head, column.tail: _*))
      .select(yColumn, "all")
      .withColumn("tuple", explode($"all"))
      .withColumn(xColumn, $"tuple"(0))
      .withColumn(valueColumnName, $"tuple"(1))
      .select(yColumn, xColumn, valueColumnName)
  }

}
