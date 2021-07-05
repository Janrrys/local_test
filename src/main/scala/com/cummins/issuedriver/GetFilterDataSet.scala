package com.cummins.issuedriver

import java.util

import com.cummins.utils.SuperJob
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * 2021/1/19
 * create by rr398 
 * READ:[table names]
 * SAVE:[table names]
 */
object GetFilterDataSet extends SuperJob {

  import spark.implicits._

  private val filterMap: util.HashMap[String, util.ArrayList[String]] = new util.HashMap[String, util.ArrayList[String]]()

  override def run(): Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\Mileagedata.json")
    val filter_df: Dataset[Row] = df.where($"REL_USER_APPL_DESC" === "School")
    filter_df.show()


    /**
     * analysis_data_quality_df
     */

    val analysis_data_quality_df: DataFrame = analysis_data_quality(df)
    analysis_data_quality_df.show(100, truncate = false)


    /**
     * parsingFilter
     */

    val filters: util.HashMap[String, util.ArrayList[String]] = parsingFilter(filter_df)

    val filter_map: mutable.Map[String, Seq[String]] = filters.asScala.map((x: (String, util.List[String])) => {
      x._1 -> x._2.asScala.toSeq
    })

    filter_map.foreach((x: (String, Seq[String])) => {
      val key: String = x._1
      val value: String = x._2.mkString(",")

      println(s"$key -> $value")
    })
  }


  /**
   * 空值率
   */

  def analysis_data_quality(df: DataFrame): DataFrame = {

    val newSchema: StructType = df.schema.add("blank", IntegerType).add("nonblank", IntegerType)
    val newRdd: RDD[Row] = df.rdd.map((row: Row) => {
      var blank = 0
      var nonblank = 0
      row.toSeq.foreach((fields: Any) => {
        if (fields == null) {
          blank += 1
        }
          nonblank += 1
      })
      val add_blank: Seq[Any] = row.toSeq :+ blank :+ nonblank
      Row.fromSeq(add_blank)
    })
    val newDataframe: DataFrame = spark.createDataFrame(newRdd, newSchema)
    newDataframe
  }


  /**
   * parsingFilter：拿到筛选条件的数据
   */

  def parsingFilter(row: DataFrame): java.util.HashMap[String, java.util.ArrayList[String]] = {

    val fieldNames: Seq[String] = Seq("ReportId", "IssueContents", "ExcludeFC", "DataSource")
    fieldNames.foreach((fieldName: String) => {
      val array: Array[Row] = row.select(fieldName).take(1)
      if (array.nonEmpty) {
        val value: String = array(0).mkString
        if ((value != null) && value.nonEmpty && !value.equals("")) {
          filterMap.put(fieldName, new java.util.ArrayList(value.split("\\|").toList.asJava))
        } else {
          filterMap.put(fieldName, new java.util.ArrayList[String]())
        }
      }
    })
    this.filterMap
  }
}
