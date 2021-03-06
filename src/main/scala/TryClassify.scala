import java.io.File
import java.util.{Calendar, Date}

import Uniom.feature

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object TryClassify {
  def main(args: Array[String]): Unit = {
    val ele=new ArrayBuffer[((String,Calendar,Calendar,Double,Double,Int))]()
    val f=new feature

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
   // lng+","+lat+","+dStart+","+dEnd+","+attri
    val groupData=ele.groupBy(x=>x._1).filter(x=>start.filterTime(x)).map(x=>start.mergeData(x)).map(x=>start.divUser(x._1,x._2.toIterable))



  }
}
