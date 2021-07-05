package com.cummins.libraries

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import scalaj.http.{Http, HttpResponse}

object Util {

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  def getSpark()={
    this.spark
  }


  val http: Http.type = Http

  def BFPostJson(url: String, data: String = "", successCallback: HttpResponse[String] => Unit) = {
    val result: HttpResponse[String] = http(url).header("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8").postData(data).postForm.asString
    if (result.code == 200) {
      successCallback(result)
    }
  }

  def BFPostJson(url: String, headers: Seq[(String, String)], data: String , successCallback: HttpResponse[String] => Unit) = {
    val result: HttpResponse[String] = http(url)
      .header("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
      .headers(headers)
      .postData(data)
      .postForm
      .asString
    if (result.code == 200) {
      successCallback(result)
    }
  }

  val dateFormatter2 = DateTimeFormatter.ofPattern("yyyyMMdd")

  def addDays(time: String, num: Int): String = {
    val dateTime = LocalDate.parse(time)
    dateTime.plusDays(num).format(dateFormatter2)
  }

  def parseTime(time: String) = {
    val date: LocalDate = LocalDate.parse(time,dateFormatter2)
    date
  }


}
