import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import JZLocationConverter.{LatLng, gcj02ToWgs84, wgs84ToGcj02}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object testSample {
  def main(args: Array[String]): Unit = {
    val ele=new ArrayBuffer[((String,Calendar,Calendar,Double,Double,Int))]()

    val start=new UnionCode

    val file=new File("E:\\study\\毕设\\测试数据\\bh603")
//    val writer=new PrintWriter(new File("E:\\study\\毕设\\测试数据\\preciseStu.csv" ))

    val fileList=file.listFiles()

    for (f <- fileList) {
      val con=Source.fromFile(f)
      for (line <- con.getLines()) {
        val t = start.parseSampleData(line)
        ele += t
      }
    }

    val groupData=ele.groupBy(x=>x._1).filter(x=>start.filterTime(x)).map(x=>start.mergeData(x)).map(x=>start.divUser(x._1,x._2.toIterable))


//    val t1=groupData.filter(x=>x._3.equals("preciseStu")).map(x=>(x._1,x._2)).flatMap(x=>x._2)
//
//    for(l<-t1)
//    {
//
//      var kind="unknown"
//
//      if((l._4>116.33531&&l._4<116.33977&&l._5>39.97503&&l._5<39.97762))
//        kind="resident1"
//      else if(l._4>116.33804&&l._4<116.34204&&l._5>39.979&&l._5<39.98515)
//        kind="resident2"
//      else if((l._4>116.3418&&l._4<116.3497&&l._5>39.9776&&l._5<39.9854))
//        kind="workTech"
//      else if(l._4>116.3406&&l._4<116.3493&&l._5>39.9746&&l._5<39.9777)
//        kind="workMall"
//
//
//      val location=wgs84ToGcj02(new LatLng(l._5,l._4))
//
//      writer.write(l._1+","+l._2.get(Calendar.HOUR_OF_DAY)+","+l._2.get(Calendar.MINUTE)
//        +","+l._3.get(Calendar.HOUR_OF_DAY)+","+l._3.get(Calendar.MINUTE)+","+location.longitude+","+location.latitude+","+l._6+","+kind)
//
//      writer.write("\n")
//    }
//    writer.close()
//    var preStuCount=0
//    var obscStuCount=0
//    var workerCount=0
//    var teacherCount=0
//    var unknownCount=0
//
//    for(l<-groupData)
//    {
//        if(l.equals("preciseStu"))
//          preStuCount=preStuCount+1
//        else if(l.equals("obscureStu"))
//          obscStuCount=obscStuCount+1
//        else if(l.equals("worker"))
//          workerCount=workerCount+1
//        else if(l.equals("teacher"))
//          teacherCount=teacherCount+1
//        else
//          unknownCount=unknownCount+1
//    }
//
//    println(preStuCount+","+obscStuCount+","+workerCount+","+teacherCount+","+unknownCount)



  }
}
