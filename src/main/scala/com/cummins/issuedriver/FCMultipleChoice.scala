package com.cummins.issuedriver

import java.util

import com.cummins.utils.SuperJob
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * 2021/4/16
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object FCMultipleChoice extends SuperJob {

  private val filterMap: util.HashMap[String, util.ArrayList[String]] = new util.HashMap[String, util.ArrayList[String]]()
  val getScalaSeq: String => Seq[String] = (key: String) => filterMap.get(key).asScala.toSeq

  import spark.implicits._

  override def run(): Unit = {

    val efpa_df: DataFrame = spark.read.json("C:\\Users\\rr398\\IdeaProjects\\local_test\\src\\FC_NonimFC2.json")
      .groupBy("ESN").agg(
      collect_list("FAULT_CODE_LIST") as "FAULT_CODE_LIST",
      collect_list("NONIM_FAULT_CODE_LIST") as "NONIM_FAULT_CODE_LIST"
    ).orderBy("ESN")
      .withColumn("ALL_FAULT_CODE_LIST", pickUpString($"FAULT_CODE_LIST", $"NONIM_FAULT_CODE_LIST"))

    efpa_df.show(100, false)


    val filter_df: DataFrame = spark
      .read
      .option("header", value = true)
      .option("sep", ";")
      .csv("C:\\Users\\rr398\\IdeaProjects\\local_test\\ISSUEDRIVERPARAMETER.csv")

    filter_df.show(100, false)


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

    val issue_df: DataFrame = get_issue_df(efpa_df).withColumn("data_type_label", lit("iss"))
    issue_df.show(100, false)
  }

  /**
   * parsingFilter：拿到筛选条件的数据
   */

  def parsingFilter(row: DataFrame): java.util.HashMap[String, java.util.ArrayList[String]] = {

    val fieldNames: Seq[String] = Seq("ReportId", "IssueType", "IssueContents", "ExcludeFC", "DataSource")
    fieldNames.foreach((fieldName: String) => {
      val array: Array[Row] = row.select(fieldName).take(1) //([202104130001],[5655|2772|5655|3398],[NULL],[BUILD|EFPA|CLAIM|GEOLOCATION])
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


  /**
   * Filter out issue_df
   *
   * @param df
   * @return
   */
  def get_issue_df(df: DataFrame): DataFrame = {

    val IssueType: String = filterMap.get("IssueType").get(0)
    val IssueContents: Array[String] = getScalaSeq("IssueContents").toArray
    val exclodeFC: Array[String] = getScalaSeq("ExcludeFC").toArray


    IssueType match {
      case "FC" =>
        val fcdf: Dataset[Row] = df.where(
          diff(lit(IssueContents), $"ALL_FAULT_CODE_LIST")
            && (!intersection($"FAULT_CODE_LIST", lit(exclodeFC)) && (!intersection($"nonim_fault_code_list", lit(exclodeFC)))))

        fcdf

      case "Parts" =>
        val fpdf: Dataset[Row] = df.where(array_contains($"REL_CMP_FAILURE_PART_LIST", filterMap.get("IssueContents").get(0)))
        fpdf

      case _ =>
        df
    }
  }


  val diff: UserDefinedFunction = udf(
    (thisarr: mutable.WrappedArray[String], otherarr: mutable.WrappedArray[String]) => {
      if (thisarr == null || thisarr.isEmpty || otherarr == null || otherarr.isEmpty) {
        false
      } else {
        val diffarray: mutable.WrappedArray[String] = thisarr diff otherarr

        println(diffarray.toString)

        if (diffarray.isEmpty) true else false
      }
    })


  val intersection: UserDefinedFunction = udf(
    (thisarr: mutable.WrappedArray[String], otherarr: mutable.WrappedArray[String]) => {
      if (thisarr == null || thisarr.isEmpty || otherarr == null || otherarr.isEmpty) {
        false
      } else {
        val intersection: mutable.WrappedArray[String] = thisarr intersect otherarr
        if (intersection.nonEmpty) true else false
      }
    })


  def pickUp = udf(
    (a: mutable.WrappedArray[String], b: mutable.WrappedArray[String]) => {
      a.toList ::: b.toList
    }
  )
  def pickUpString = udf(
    (a:mutable.WrappedArray[String],b:mutable.WrappedArray[String])=>{
      if (a != null && a.nonEmpty) {
        if (b == null || b.isEmpty) {
          a.toList
        }
        else a.toList ::: b.toList
      }
      else if ((a == null || a.isEmpty) && b != null && b.nonEmpty) {
        b.toList
      }
      else null
    }
  )


}
