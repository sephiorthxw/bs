package Uniom

import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object testLcss {
  def main(args: Array[String]): Unit = {

    val con1=Source.fromFile("E:\\study\\毕设\\测试数据\\lcssTest1.csv")
    val con2=Source.fromFile("E:\\study\\毕设\\测试数据\\lcssTest2.csv")

    val store1=new ArrayBuffer[stopPoint]()
    val store2=new ArrayBuffer[stopPoint]()

    for (line <- con1.getLines()) {
      val ele = line.split(",")
      val id = ele(0)
      val lng = ele(5).toDouble
      val lat = ele(6).toDouble
      val sH = ele(1).toInt
      val sM = ele(2).toInt
      val eH = ele(3).toInt
      val eM = ele(4).toInt

      var sDate = new Date()
      sDate.setHours(sH)
      sDate.setMinutes(sM)

      var eDate = new Date()
      eDate.setHours(eH)
      eDate.setMinutes(eM)

      store1 += new stopPoint(lng, lat, sDate, eDate, "")
    }


    for (line <- con2.getLines()) {
      val ele = line.split(",")
      val id = ele(0)
      val lng = ele(5).toDouble
      val lat = ele(6).toDouble
      val sH = ele(1).toInt
      val sM = ele(2).toInt
      val eH = ele(3).toInt
      val eM = ele(4).toInt

      var sDate = new Date()
      sDate.setHours(sH)
      sDate.setMinutes(sM)

      var eDate = new Date()
      eDate.setHours(eH)
      eDate.setMinutes(eM)

      store2 += new stopPoint(lng, lat, sDate, eDate, "")
    }

    val f=new feature
    println(f.routeSimlar(("1",store1),("2",store2)))

  }
}
