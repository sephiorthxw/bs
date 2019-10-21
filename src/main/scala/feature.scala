import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

class feature {


  def calcDis(lng1:Double,lat1:Double,lng2:Double,lat2:Double):Double={
    val r=6371393 //地球半径
    val con=Math.PI/180.0 //用于计算的常量
    val radLat1: Double = lat1*con
    val radLat2: Double = lat2*con
    val a: Double = radLat1 - radLat2
    val b: Double = lng1*con - lng2*con

    var s: Double = 2.0 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2.0), 2.0) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2.0), 2.0)))
    s = s * r
    s = Math.round(s * 10000.0) / 10000.0
    s
  }



  /*
    出行时间
   */
  def moveTime(line:(String, Iterable[stopPoint])): (String, Long) ={
    var res=0L
    val ele=line._2.toArray.sortBy(x=>x.dStart)
    val length=ele.length
    if(length<2)
      (line._1,res)
    else
    {
      for(i<-0 until length-1)
      {
        val stime=ele(i).dStart.getTime
        val etime=ele(i+1).dEnd.getTime
        res=res+(etime-stime)
      }
      (line._1,res)
    }


  }


//  /*
//    工作时间
//   */
//
//  def workTime(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={
//    var resWork=0L
//    var resHome=0L
//    for(s<-line._2)
//    {
//      if(s._6.equals("work"))
//      {
//        resWork=resWork+(s.lat.getTime-s.lng.getTime)
//      }
//
//      if(s._6.equals("home"))
//      {
//        resHome=resHome+(s.lat.getTime-s.lng.getTime)
//      }
//    }
//
//    (line._1,resWork,resHome)
//  }

  def judgeInOut(line:(String, Iterable[stopPoint]))={
    val ele=line._2.toArray.sortBy(x=>x.dStart)
    var res=false

    if(ele.length<2)
      res
    else
    {
      if(ele(0).attri.equals("home")&&ele(ele.length-1).attri.equals("home"))
        res=true
    }

    res
  }


  /*
    出门时间，回家时间
   */
  def inOutTime(line:(String, Iterable[stopPoint]))={
    val ele=line._2.toArray.sortBy(x=>x.dStart)
    var outTime=0.0
    var inTime=0.0
    var mark=true


    if(ele.length==1)
      (0.0,0.0)
    else
    {
      outTime=ele(0).dEnd.getHours
      var j=ele.length-1

      while(j>0&&mark)
      {
        if(calcDis(ele(j).lng,ele(j).lat,ele(0).lng,ele(0).lat)>300)
        {
          if(j==ele.length-1)
          {
            inTime=ele(j).dEnd.getHours

          }
          else
          {
            inTime=ele(j+1).dStart.getHours
          }
          mark=false
        }
        else
        {
          j=j-1
        }
      }

      if(j==0&&mark)
      {
        (0.0,0.0)
      }
      else
      {
        (outTime,inTime)
      }

    }
  }

  /*
    路径相似性
    */

  def routeSimlar(line1:(String, Iterable[stopPoint]),
                  line2:(String, Iterable[stopPoint]))={


    var ele1=new Array[ArrayBuffer[Double]](144)
    var ele2=new Array[ArrayBuffer[Double]](144)

    for(s<-ele1)
    {
      s+=0.0
      s+=0.0
    }
    for(s<-ele2)
    {
      s+=0.0
      s+=0.0
    }


    for(l<-line1._2)
    {
      val stime=l.dStart.getHours*6+l.dStart.getMinutes
      val etime=l.dEnd.getHours*6+l.dEnd.getMinutes
      val lng=l.lng
      val lat=l.lat
      for(index<- stime to etime)
      {
        ele1(index)(0)=lng
        ele1(index)(1)=lat
      }
    }

    for(l<-line2._2)
    {
      val stime=l.dStart.getHours*6+l.dStart.getMinutes
      val etime=l.dEnd.getHours*6+l.dEnd.getMinutes
      val lng=l.lng
      val lat=l.lat
      for(index<- stime to etime)
      {
        ele2(index)(0)=lng
        ele2(index)(1)=lat
      }

    }

    var eleBuffer1=new ArrayBuffer[ArrayBuffer[Double]]()
    var eleBuffer2=new ArrayBuffer[ArrayBuffer[Double]]()

    for(i<-0 to 143)
    {
      if(ele1(i)(0)!=0.0&&ele2(i)(0)!=0.0)
      {
        eleBuffer1+=ele1(i)
        eleBuffer2+=ele2(i)
      }
    }

    val l=eleBuffer1.length+1
    var dp = new Array[ArrayBuffer[Int]](l)
    for(i <- 0 until dp.length){
      dp(i) = new ArrayBuffer[Int](l)
    }

    for(i <- 0 until l-1)
    {
      for(j <- 0 until l-1)
      {
        if(calcDis(eleBuffer1(0)(0),eleBuffer1(0)(1),eleBuffer2(0)(0),eleBuffer2(0)(1))<100)
        {
          dp(i+1)(j+1)=1+dp(i)(j)
        }
        else
        {
          dp(i+1)(j+1)=Math.max(dp(i+1)(j),dp(i)(j+1))
        }
      }
    }


    val d=dp(l-1)(l-1).toDouble

    1-d/l-1

  }


  /*
 移动距离
 */
  def dis(line:(String, Iterable[stopPoint]))={
    val ele=line._2.toArray
    var res=0.0
    var calc=0.0
    if(ele.size<2)
      (line._1,res)
    else
    {
      val len=ele.length
      for(i<-0 until len-1)
      {
        val next=ele(i+1)
        val cur=ele(i)
        calc=calcDis(next.lng,next.lat,cur.lng,cur.lat)
        res=res+calc
      }
      (line._1,res)
    }

  }



  def freq(line:(String, Iterable[stopPoint]))={
      line._2.size.toDouble-1
  }

  /*
   判断两个矩形是否相交
   */
  def judgeIsOverlap(rlng1:Double,rlat1:Double,llng1:Double,llat1:Double,rlng2:Double,rlat2:Double,llng2:Double,llat2:Double): Boolean=
  {
    val wid1=rlng1-llng1
    val heigh1=rlat1-llat1
    val wid2=rlng2-llng2
    val heigh2=rlat2-llat2

    val midLng1=(rlng1+llng1)/2
    val midLat1=(rlat1+llat1)/2
    val midLng2=(rlng2+llng2)/2
    val midLat2=(rlat2+llat2)/2

    val condition1=(Math.abs(midLng1-midLng2)<=(wid1+wid2)/2)
    val condition2=(Math.abs(midLat1-midLat2)<=(heigh1+heigh2)/2)

    var res=false

    if(condition1&&condition2)
      res=true

    res
  }


 /*
   判断某个地点旁是否有学校类别的POI
  */
  def isSchool(coord:(Double,Double),line:Iterable[(Double,Double,Double,Double,Character,Character)])={
    var isInclude=false
    var isOverLap=false
    var rlng=0.0
    var rlat=0.0
    var llng=0.0
    var llat=0.0

    val clng=coord._1
    val clat=coord._2

    var loop1=new Breaks
    loop1.breakable {
      for (l <- line) {
        rlng = l._1
        rlat = l._2
        llng = l._3
        llat = l._4
        if (clng>llng&&clng<rlng&&clat>llat&&clat<llat)
          {
              isInclude=true
              loop1.break()
          }
      }
    }

    if(isInclude)
        true
    else
    {
       //将中心坐标上下左右扩充200米
        val virtualRLng=clng+0.0018
        val virtualRLat=clat+0.0018
        val virtualLLng=clng-0.0018
        val virtualLLat=clat-0.0018

       //判断扩充后的区域是否与学校区域有交集
        var loop2=new Breaks
        loop2.breakable{
            for(l<-line)
              {
                rlng = l._1
                rlat = l._2
                llng = l._3
                llat = l._4

                if(judgeIsOverlap(virtualRLng,virtualRLat,virtualLLng,virtualLLat,rlng,rlat,llng,llat))
                  {
                    isOverLap=true
                    loop2.break()
                  }
              }
        }
      isOverLap

     }

  }

  def homeWorkRate(line:(String,Iterable[stopPoint])):(Double,Double) =
  {
    val tmp = line._2.toArray
    val tmpC = tmp(0).dStart


    val tmpTime = Calendar.getInstance()
    tmpTime.setTime(tmpC)

    val resi1STime = Calendar.getInstance()
    val resi1ETime = Calendar.getInstance()
    resi1STime.set(tmpTime.get(Calendar.YEAR), tmpTime.get(Calendar.MONTH), tmpTime.get(Calendar.DAY_OF_MONTH), 0, 0, 0)
    resi1ETime.set(tmpTime.get(Calendar.YEAR), tmpTime.get(Calendar.MONTH), tmpTime.get(Calendar.DAY_OF_MONTH), 7, 50, 0)


    val resi2STime = Calendar.getInstance()
    val resi2ETime = Calendar.getInstance()
    resi2STime.set(tmpTime.get(Calendar.YEAR), tmpTime.get(Calendar.MONTH), tmpTime.get(Calendar.DAY_OF_MONTH), 21, 0, 0)
    resi2ETime.set(tmpTime.get(Calendar.YEAR), tmpTime.get(Calendar.MONTH), tmpTime.get(Calendar.DAY_OF_MONTH), 23, 50, 0)

    val resiInterval=(resi2ETime.getTimeInMillis-resi2STime.getTimeInMillis)+(resi1ETime.getTimeInMillis-resi1STime.getTimeInMillis)


    val workSTime = Calendar.getInstance()
    val workETime = Calendar.getInstance()
    workSTime.set(tmpTime.get(Calendar.YEAR), tmpTime.get(Calendar.MONTH), tmpTime.get(Calendar.DAY_OF_MONTH), 8, 0, 0)
    workETime.set(tmpTime.get(Calendar.YEAR), tmpTime.get(Calendar.MONTH), tmpTime.get(Calendar.DAY_OF_MONTH), 20, 50, 0)


    val workInterval=workETime.getTimeInMillis-workSTime.getTimeInMillis


    var homeTime=0L
    var workTime=0L
    var work2Time=0L

    for(l<-line._2)
    {
      val stime = Calendar.getInstance()
      val etime = Calendar.getInstance()



      stime.setTime(l.dStart)
      etime.setTime(l.dEnd)
      // todo 居住区范围
      if((l.lng>116.33531&&l.lng<116.33977&&l.lat>39.97503&&l.lat<39.97762)||(l.lng>116.33804&&l.lng<116.34204&&l.lat>39.979&&l.lat<39.98515)) {
        if (stime.before(resi1ETime)) {
          if (etime.before(resi1ETime))
            homeTime = homeTime + (etime.getTimeInMillis - stime.getTimeInMillis)
          else
            homeTime = homeTime + (resi1ETime.getTimeInMillis - stime.getTimeInMillis)
        }
        else if (etime.after(resi2STime)) {
          if (stime.before(resi2STime)) {
            homeTime = homeTime + (etime.getTimeInMillis - resi2STime.getTimeInMillis)
          }
          else {
            homeTime = homeTime + (etime.getTimeInMillis - stime.getTimeInMillis)
          }

        }
      }

      // todo 工作区区范围
      if((l.lng>116.3418&&l.lng<116.3497&&l.lat>39.9776&&l.lat<39.9854))
      {
        if(stime.after(workSTime)&&etime.before(workETime))
          workTime=workTime+(etime.getTimeInMillis-stime.getTimeInMillis)
        else if(stime.before(workSTime)&&etime.after(workETime))
          workTime=workTime+(workETime.getTimeInMillis-workSTime.getTimeInMillis)
        else if(stime.before(workSTime)&&etime.after(workSTime)&&etime.before(workETime))
          workTime=workTime+(etime.getTimeInMillis-workSTime.getTimeInMillis)
        else if(stime.before(workETime)&&stime.after(workSTime)&&etime.after(workETime))
          workTime=workTime+(workETime.getTimeInMillis-stime.getTimeInMillis)
      }

      if((l.lng>116.3406&&l.lng<116.3493&&l.lat>39.9746&&l.lat<39.9777))
      {
        if(stime.after(workSTime)&&etime.before(workETime))
          work2Time=work2Time+(etime.getTimeInMillis-stime.getTimeInMillis)
        else if(stime.before(workSTime)&&etime.after(workETime))
          work2Time=work2Time+(workETime.getTimeInMillis-workSTime.getTimeInMillis)
        else if(stime.before(workSTime)&&etime.after(workSTime)&&etime.before(workETime))
          work2Time=work2Time+(etime.getTimeInMillis-workSTime.getTimeInMillis)
        else if(stime.before(workETime)&&stime.after(workSTime)&&etime.after(workETime))
          work2Time=work2Time+(workETime.getTimeInMillis-stime.getTimeInMillis)
      }

    }

    val homeRate=homeTime.toDouble/resiInterval.toDouble

    val workRate=workTime.toDouble/workInterval.toDouble

    val work2Rate=work2Time.toDouble/workInterval.toDouble


    (homeRate,workRate+work2Rate)
  }

  def calcOneDayFeature(line:(String,Iterable[stopPoint]))={

       val inOutHomeTime=inOutTime(line)
       val movTime=moveTime(line)
       val hwRate=homeWorkRate(line)
       val moveDis=dis(line)
      (inOutHomeTime._1,inOutHomeTime._2,movTime._2,hwRate._1,hwRate._2,moveDis._2)
  }



  def main()={
     val conf=new SparkConf().setAppName("test")
     val sc=new SparkContext(conf)
     var rdd1=sc.textFile("hdfs://bigdata01:9000/home/xw/test/2014moblieData/{[1-5]}.csv")
     var a=3
     var rdd2=rdd1.map{
       x=>
           val ele=x.split(",")
           val id=ele(0)
           val lng=ele(1).toDouble
           val lat=ele(2).toDouble
           val sdate=new Date(ele(3))
           val edate=new Date(ele(4))
           val kind=ele(5)
           (id,lng,lat,sdate,edate,kind)
         }.map(x=>(x._1,x)).groupByKey().map{
       x=>
       var ele=x._2.map(x=>(x._4.getDay,x)).groupBy(x=>x._1)
         (x._1,ele)
     }


  }



}
