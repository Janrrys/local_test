package com.cummins.Section5_functions

/**
 * 2021/2/25
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object FunctionTest {

  private val jiami= (x: Int) => (x * 2 + 1) / 4
  private val jiemi: Int => Int = (x: Int) => (x * 4 - 1) / 2
  val lilv2021 = 0.002

  def main(args: Array[String]): Unit = {

    val idCard: Array[Int] = Array(1, 2, 3, 4).map(_ + 1)


    val telNumber: Array[Int] = Array(139, 132, 3, 4).map(jiami)

    idCard map jiemi mkString ","

    println(idCard.mkString(","))
    println(telNumber.mkString(","))

    val multiplyBy = (factor:Double) => { (x:Double)=>factor*x }

    val lixi2020: Double => Double = multiplyBy(0.001)



    val 佳营的利息: Double = lixi2020(10000)
    val 方兴的利息: Double = lixi2020(100000)



    def 计算2021利息(本金:Double) = {
      本金 * lilv2021
    }


//    def multiplyBy(factor:Double)(x:Double) = factor*x
//
//
//    val multiplyByRatio: Double => Double = multiplyBy(0.8)
//
//    val d: Double = multiplyByRatio(1000)










    getWithCallback("www.jiaying.com/api/user?name=jiaying",(x:String)=>{
      val strings: Array[String] = x.split("\\|")
      println(s" name = ${strings(0)} , age = ${strings(1)}")
    })


    val kegouma: Any = getWithCallback("www.jiaying.com/api/commodity?code=109601", (data: String) => {
      val strings: Array[String] = data.split("\\|")
      //'109601|￥100|10'
      if (strings(3).toInt > 0) {
        true
      } else false

    })

    if (kegouma.asInstanceOf[Boolean]) {
      println("可以购买")
    }else println("库存不足")







  }


//  def convertIntToString(f:(Int)=>String,x:Int)=f(x)

  def getWithCallback(url:String,success:String=>Any)={
   val str: String = get(url)
    val result: Array[String] = str.split(",")
    if (result(0).equals("200")) {
      val data: String = result(2)

      success(data)

    } else throw new Exception(result(1))

  }




  def get(url:String)={
    ""
    //"200,success,'jiaying|18|g|165'"
    //"209,Unknown user,'' "
  }



}
