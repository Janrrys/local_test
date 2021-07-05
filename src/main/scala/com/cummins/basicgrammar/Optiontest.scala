package com.cummins.basicgrammar

/**
 * 2021/2/22
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object Optiontest {
  def main(args: Array[String]): Unit = {
    val map: Map[String, String] = Map("key1" -> "value1")
    val value1: Option[String] = map.get("key1")
    val value2: Option[String] = map.get("key2")

    println(value1) //Some(value1)
    println(value2) //None


    val sites: Map[String, String] = Map("runoob" -> "www.runoob.com", "google" -> "www.google.com")
    println("sites.get( \"runoob\" ) : " +  sites.get( "runoob" ))
    println("sites.get( \"baidu\" ) : " +  sites.get( "baidu" ))
  }

}
