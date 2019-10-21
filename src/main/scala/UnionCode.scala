import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer

class UnionCode {

  def judgeData(line:String):Boolean={

    val format=new SimpleDateFormat("yyyyMMddHHmmss")

    var judgeLength=0
    var judgeTime=0
    var judgeLoc=0
    var judgeScope=0
    var isVaild=0


    try{
      val eleTime=format.parse(line.split(",")(1))
      val lng=line.split(",")(3).toDouble
      val lat=line.split(",")(4).toDouble
      val interval=line.split(",")(5)

      if(interval.equals("0"))
        isVaild=1

      if(lat<39.4333||lat>41.05||lng<115.41667||lng>117.5)
      {
        judgeScope=1
      }

    }
    catch {
      case exLength:java.lang.ArrayIndexOutOfBoundsException=>{
        judgeLength=1
      }
      case ex:java.text.ParseException=>{
        judgeTime=1
      }
      case exLoc:java.lang.NumberFormatException=>{
        judgeLoc=1
      }
    }
    if(judgeTime==1||judgeLoc==1||judgeLength==1||judgeScope==1||isVaild==1)
      false
    else
      true
  }

  def parseData(line:String)={
    val format=new SimpleDateFormat("yyyyMMddHHmmss")
    val ele=line.split(",")
    val id=ele(0)
    val sdate=format.parse(ele(1))
    val edate=format.parse(ele(2))

    val stime = Calendar.getInstance
    val etime = Calendar.getInstance
    stime.setTime(sdate)
    etime.setTime(edate)

    val lng=ele(3).toDouble
    val lat=ele(4).toDouble

    val interval=ele(5).toInt

    (id,stime,etime,lng,lat,interval)
  }


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

  def filterUser(line:(String,Iterable[(String,Calendar,Calendar,Double,Double,Int)])): Boolean =
  {
    val tmp = line._2.toArray
    val tmpTime = tmp(0)._2

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

    for(l<-line._2)
    {
      val stime=l._2
      val etime=l._3
      // todo 居住区范围
      if((l._4>116.33531&&l._4<116.33977&&l._5>39.97503&&l._5<39.97762)||(l._4>116.33804&&l._4<116.34204&&l._5>39.979&&l._5<39.98515)) {
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
      if((l._4>116.3418&&l._4<116.3497&&l._5>39.9776&&l._5<39.9854)||(l._4>116.3406&&l._4<116.3493&&l._5>39.9746&&l._5<39.9777))
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

    }

    val homeRate=homeTime.toDouble/resiInterval.toDouble

    val workRate=workTime.toDouble/workInterval.toDouble

    var res=false
    if((homeRate>0.7)&&(workRate>0.5))
      res=true
    else
      res=false

    res
  }


  def filterBhUser(line:(String,Iterable[(String,Calendar,Calendar,Double,Double,Int)])): Unit =
  {

          var bhTime=0

         for(l<-line._2)
           {
                //todo 北航区域范围
                if(true)
                   bhTime=bhTime+l._6

           }

         if(bhTime>600)
             true
         else
             false
  }

  def parseSampleData(line:String)={
    //第一个'['字符的位置
    val firstI=line.indexOf('[')
    //'['字符后第一个逗号的位置
    val firstD=line.indexOf(',',firstI)
    //起始时间
    val stimeString=line.substring(firstI+6,firstD).toLong
    val stime = Calendar.getInstance()
    stime.setTimeInMillis(stimeString)

    val secondI=line.indexOf('[',firstD)
    val thirdI=line.indexOf('[',secondI+1)
    val secondD=line.indexOf(',',thirdI)
    val etimeString=line.substring(thirdI+6,secondD).toLong

    val etime = Calendar.getInstance()
    etime.setTimeInMillis(etimeString)

    //第一个']'
    val lastO=line.lastIndexOf(']')


    val ele=line.substring(lastO+2).split(",")


    val startD=line.indexOf(',')
    val id=line.substring(0,startD)

    (id,stime,etime,ele(0).toDouble,ele(1).toDouble,ele(2).toInt)

  }

  def divUser(line:(String,Iterable[(String,Calendar,Calendar,Double,Double,Int)])):(String,Iterable[(String,Calendar,Calendar,Double,Double,Int)],String) =
  {
    val tmp = line._2.toArray
    val tmpTime = tmp(0)._2

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
      val stime=l._2
      val etime=l._3
      // todo 居住区范围
      if((l._4>116.33531&&l._4<116.33977&&l._5>39.97503&&l._5<39.97762)||(l._4>116.33804&&l._4<116.34204&&l._5>39.979&&l._5<39.98515)) {
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
      if((l._4>116.3418&&l._4<116.3497&&l._5>39.9776&&l._5<39.9854))
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

      if((l._4>116.3406&&l._4<116.3493&&l._5>39.9746&&l._5<39.9777))
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

    var kind="unknown"
    if(homeRate>0.7)
      {
            if(workRate>0.5)
               kind="preciseStu"
            else
               kind="obscureStu"
      }
    else
    {
           if(work2Rate>0.7&&homeRate<0.2)
              kind="worker"
           else if(workRate>0.7&&homeRate<0.2)
              kind="teacher"
    }

    (line._1,line._2,kind)
  }


  def filterTime(line:(String,Iterable[(String,Calendar,Calendar,Double,Double,Int)]))={
       var time=0
    for(l<-line._2)
       {
           time=time+l._6
       }

       var res=false
       if(time>=1200)
         res=true
       else
         res=false


       res

  }


  def mergeData(line:(String,Iterable[(String,Calendar,Calendar,Double,Double,Int)])):(String,Iterable[(String,Calendar,Calendar,Double,Double,Int)])={

    val resXl=new ArrayBuffer[(String,Calendar,Calendar,Double,Double,Int)]()

    val tmp=line._2.toArray
    val minute=60*1000
    var i=0
    var start=0
    while(i<tmp.size-1)
    {
      val cur=tmp(i)
      val lng=cur._4
      val lat=cur._5

      val forwardLng=tmp(i+1)._4
      val fowardLat=tmp(i+1)._5

      if(lng==forwardLng&&lat==fowardLat)
      {
        i=i+1
      }
      else
      {
        if(i==start)
        {
          resXl+=cur
          i=i+1
          start=start+1
        }
        else
        {
          val stime=tmp(start)._2
          val etime=cur._3

          val ems=etime.get(Calendar.HOUR_OF_DAY)*60+etime.get(Calendar.MINUTE)
          val sms=stime.get(Calendar.HOUR_OF_DAY)*60+stime.get(Calendar.MINUTE)

          val intervaltmp=(ems-sms).toDouble
          val interval=intervaltmp
          val ele=(cur._1,stime,etime,cur._4,cur._5,interval.toInt)
          resXl+=ele
          i=i+1
          start=i
        }
      }

    }

    if(start<i)
    {
      val stime=tmp(start)._2
      val etime=tmp(i)._3
      val interval=(etime.get(Calendar.MILLISECOND)-stime.get(Calendar.MILLISECOND))/1000*60
      val ele=(tmp(i)._1,stime,etime,tmp(i)._4,tmp(i)._5,interval)
      resXl+=ele
    }
    (line._1,resXl.toIterable)
  }
}
