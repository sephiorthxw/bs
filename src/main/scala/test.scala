import java.text.SimpleDateFormat

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import java.util.Date

import scala.collection.mutable
import scala.io.Source
import scala.util.control.Breaks
import org.apache.spark.mllib
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors


object test {

  def judgeUserV1(line:(String,Iterable[String])):Boolean={


      var userId=line._1
      val messge=line._2
      val format=new SimpleDateFormat("yyyyMMddHHmmss")


      var count0to7=0
      var count7to10=0
      var count10to13=0
      var count13to16=0
      var count16to19=0
      var count19to24=0

      for(s<-messge)
      {
        try{
          val eleHour=format.parse(s.split(",")(2)).getHours
          if(eleHour>=0&&eleHour<7)
            count0to7+=1
          else if(eleHour>=7&&eleHour<10)
            count7to10+=1
          else if(eleHour>=10&&eleHour<13)
            count10to13+=1
          else if(eleHour>=13&&eleHour<16)
            count13to16+=1
          else if(eleHour>=16&&eleHour<19)
            count16to19+=1
          else
            count19to24+=1
        }
        catch
          {
            case ex:Exception=>{
              println(s)
            }

          }
    }
    if(count0to7>=1&&count7to10>=1&&count10to13>=1&&count13to16
      >=1&&count16to19>=1&&count19to24>=1)
      true
    else
      false


  }

  def judgeUserV2(line:(String,Iterable[String])):Boolean={

    var userId=line._1
    val messge=line._2
    val format=new SimpleDateFormat("yyyyMMddHHmmss")

    var count0to7=0
    var count7to10=0
    var count11to14=0
    var count15to18=0
    var count19to24=0

    for(s<-messge)
    {
      try{
        val eleHour=format.parse(s.split(",")(2)).getHours
        if(eleHour>=0&&eleHour<7)
          count0to7+=1
        else if(eleHour>=7&&eleHour<=10)
          count7to10+=1
        else if(eleHour>=11&&eleHour<=14)
          count11to14+=1
        else if(eleHour>=15&&eleHour<=18)
          count15to18+=1
        else
          count19to24+=1
      }
      catch
        {
          case ex:Exception=>{
            println(s)
          }

        }
    }
    if(count0to7>=1&&count7to10>=1&&count11to14>=1&&count15to18
      >=1&&count19to24>=1)
      true
    else
      false


  }

  def conVert(line:(String,Iterable[String])):(String,Iterable[String])={

    val res = new scala.collection.mutable.ArrayBuffer[String]()

    for(s<-line._2)
      {
        res+=s.split(",")(2)

      }
    (line._1,res)
  }

/*  def sort(line:(String,Iterable[String])):(String,Iterable[String])={
    val userId=line._1
    val messge=line._2
    val format=new SimpleDateFormat("yyyyMMddHHmmss")
    val date=format.parse()

  }*/

  def judgeData(line:String):Boolean={

    val format=new SimpleDateFormat("yyyyMMddHHmmss")

    var judgeLength=0
    var judgeTime=0
    var judgeLoc=0
    var judgeScope=0

    try{
    val eleTime=format.parse(line.split(",")(2))
    val lng=line.split(",")(4).toDouble
    val lat=line.split(",")(5).toDouble
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
    if(judgeTime==1||judgeLoc==1||judgeLength==1||judgeScope==1)
       false
    else
       true
  }

  def sortByTime(line:(String,Iterable[String])):(String,Iterable[String])={
    var ele=line._2.toArray
    val format=new SimpleDateFormat("yyyyMMddHHmmss")

    (line._1,ele.sortBy(x=>(format.parse(x.split(",")(2)))))
  }

  def deleteShakeV3(line:(String,Iterable[String])):(String,Iterable[(Date,Double,Double)])={
    val format=new SimpleDateFormat("yyyyMMddHHmmss")
    val ele=line._2.map{x=>
      val ele=x.split(",")
      (format.parse(ele(2)),ele(4).toDouble,ele(5).toDouble)
    }.toBuffer

    if(ele.size<3)
      (line._1,ele)
    else
      {

        var dBefore=0.0
        var dAfter=0.0
        var tBefore:Long=0
        var tAfter:Long=0
        val recordIndex=new ListBuffer[(Date,Double,Double)]

        recordIndex+=ele(0)
        for(i<- 1 to ele.length-2 )
        {
             dBefore=calcDis(ele(i-1)._2,ele(i-1)._3,ele(i)._2,ele(i)._3)
             dAfter=calcDis(ele(i)._2,ele(i)._3,ele(i+1)._2,ele(i+1)._3)
             tBefore=(ele(i)._1.getTime-ele(i-1)._1.getTime)/1000
             tAfter=(ele(i+1)._1.getTime-ele(i)._1.getTime)/1000

          if(!(dBefore>3000&&dAfter>3000&&tBefore<40&&tAfter<40))
            recordIndex+=ele(i)
        }
        recordIndex+=ele(ele.length-1)


        (line._1,recordIndex)

      }
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


  def retrieve_neighborsST(index_center:Int, df:Array[(Int,Date,Double,Double,Array[Int])],
                         spatial_threshold:Double, temporal_threshold:Long)={

        val center_point=df.filter(x=>x._1==index_center)(0)
        val max_time=center_point._2.getTime+temporal_threshold
        val min_time=center_point._2.getTime-temporal_threshold

        val df1=df.filter(x=>x._2.getTime<max_time&&x._2.getTime>min_time)
        val df2=df1.filter(x=>calcDis(center_point._3,center_point._4,x._3,x._4)<=spatial_threshold)

        df2.filter(x=>x._1!=index_center)

  }

  def retrieve_neighborsT(index_center:Int, df:Array[(Int,Date,Double,Double,Array[Int])],
                           spatial_threshold:Double, temporal_threshold:Long)={


    val res=new scala.collection.mutable.ArrayBuffer[(Int,Date,Double,Double,Array[Int])]
    val empty=new scala.collection.mutable.ArrayBuffer[(Int,Date,Double,Double,Array[Int])]
    var forward=index_center
    var backward=index_center

    val loop1=new Breaks
    val loop2=new Breaks

    loop1.breakable{
    while(backward>0)
      {
          backward-=1
          if(calcDis(df(backward)._3,df(backward)._4,df(index_center)._3,df(index_center)._4)<=spatial_threshold)
            {
              res+=df(backward)
            }
        else
            loop1.break()
      }
    }

    loop2.breakable{
    while(forward<df.length-1)
      {
         forward+=1
        if(calcDis(df(forward)._3,df(forward)._4,df(index_center)._3,df(index_center)._4)<=spatial_threshold)
        {
          res+=df(forward)
        }
        else
          loop2.break()

      }
    }
    res+=df(index_center)
    res.sortBy(x=>x._2)
    if(res(res.length-1)._2.getTime-res(0)._2.getTime<temporal_threshold)
       empty
    else if(res.length<2)
       empty
    else
       res.filter(x=>x._1!=index_center)

  }

  case class cellData(id:String,date: Date,lng:Double,lat:Double)
 // case class stopPoint(lng:Double,lat:Double,dStart:Date,dEnd:Date,attri:String)
  case class movePoint(lng:Double,lat:Double,dmove:Date)




  def tDbscanAndJudgeAttri(line:(String,Iterable[cellData]),spatial_threshold:Double,
                           temporal_threshold:Long,min_neighbors:Int)={

    var index= -1
    var clusterIndex=0
    var stack=new mutable.Stack[Int]()


    /*
      -1表示为未标记
      0表示离群点
      1....n表示簇集的id
     */
    val df=line._2.map { x =>
      val kind = Array(-1)
      index+=1
      (index,x.date,x.lng,x.lat,kind)
    }.toArray

    for(data<-df)
    {
      if(data._5(0) == -1) {
        var neighbor = retrieve_neighborsT(data._1, df, spatial_threshold, temporal_threshold)
        if(neighbor.length<min_neighbors)
          data._5(0)=0
        else if(neighbor(neighbor.length-1)._2.getTime-neighbor
        (0)._2.getTime<temporal_threshold)
          data._5(0)=0
        else{
          neighbor.remove(data._1)
          clusterIndex+=1
          data._5(0)=clusterIndex

          for(dataNeighbor<-neighbor)
          {
            dataNeighbor._5(0)=clusterIndex
            stack.push(dataNeighbor._1)
          }

          while (stack.isEmpty==false)
          {
            val cur=stack.pop()
            val newNeighbor=retrieve_neighborsT(cur,df,
              spatial_threshold,temporal_threshold
            )
            if(newNeighbor.length>=min_neighbors)
            {
              for(s<-newNeighbor)
              {
                if(s._5(0)== -1||s._5(0)==0)
                {
                  s._5(0)=clusterIndex
                  stack.push(s._1)
                }
              }
            }
          }
        }

      }

    }
    /*
      输出格式：
      停留点：（中心lng,中心lat,(停留开始时间，停留结束时间)，STOP）
      移动点： (lng,lat,（移动发生时间，移动发生时间）,MOVE)
     */
    val stop=df.groupBy(x=>x._5(0)).filter(x=>x._1!=0).map{x=>


      var clng=0.0
      var clat=0.0
      val l=x._2.sortBy(t=>t._2)
      for(y<-l)
      {
        clng+=y._3
        clat+=y._4
      }
      new stopPoint(clng/l.length,clat/l.length,l(0)._2,l(l.length-1)._2,
        judgePointAttri(l(0)._2,l(l.length-1)._2))

    }

    val move=df.filter(x=>x._5(0)==0).map{
      x=>
        movePoint(x._3,x._4,x._2)
    }
    //用户id,停留点集合,移动点集合


    (line._1,stop,move)

  }



  def tDbscanAndHW(line:(String,Iterable[cellData]),spatial_threshold:Double,
                           temporal_threshold:Long,min_neighbors:Int)={

    var index= -1
    var clusterIndex=0
    var stack=new mutable.Stack[Int]()


    /*
      -1表示为未标记
      0表示离群点
      1....n表示簇集的id
     */
    val df=line._2.map { x =>
      val kind = Array(-1)
      index+=1
      (index,x.date,x.lng,x.lat,kind)
    }.toArray

    for(data<-df)
    {
      if(data._5(0) == -1) {
        var neighbor = retrieve_neighborsT(data._1, df, spatial_threshold, temporal_threshold)
        if(neighbor.length<min_neighbors)
          data._5(0)=0
        else if(neighbor(neighbor.length-1)._2.getTime-neighbor
        (0)._2.getTime<temporal_threshold)
          data._5(0)=0
        else{
          neighbor.remove(data._1)
          clusterIndex+=1
          data._5(0)=clusterIndex

          for(dataNeighbor<-neighbor)
          {
            dataNeighbor._5(0)=clusterIndex
            stack.push(dataNeighbor._1)
          }

          while (stack.isEmpty==false)
          {
            val cur=stack.pop()
            val newNeighbor=retrieve_neighborsT(cur,df,
              spatial_threshold,temporal_threshold
            )
            if(newNeighbor.length>=min_neighbors)
            {
              for(s<-newNeighbor)
              {
                if(s._5(0)== -1||s._5(0)==0)
                {
                  s._5(0)=clusterIndex
                  stack.push(s._1)
                }
              }
            }
          }
        }

      }

    }
    val res=df.filter(x=>x._5(0)!= -1).map(x=>x._5(0)).toSeq.size

    if(res>1)
      false
    else
      true

  }
  def convertDay(line:(String,Iterable[Int])): Tuple2[String,Int] =
  {
       var res=line._2.toSet
    
       (line._1,res.size)

  }

  def judgePointAttri(start:Date,end:Date)={
      val threeH= 3*60*60*1000
      val twoH= 2*60*60*1000
      val tmp=start
      var attri="unknown"
    /*
      定义居家时间段与工作时间段
     */
      val workTime=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,7,0,0),
        new Date(tmp.getYear,tmp.getMonth,tmp.getDate,19,0,0))
      val homeTime1=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,0,0,0),
        new Date(tmp.getYear,tmp.getMonth,tmp.getDate,7,0,0))
      val homeTime2=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,19,0,0),
        new Date(tmp.getYear,tmp.getMonth,tmp.getDate,24,0,0))

      /*
        计算工作时间段交集
       */
      var Intersection=0.0
      var ts=0L
      var te=0L
      if(start.before(workTime._2)&&end.after(workTime._1))
        {
            if(start.before(workTime._1))
              ts=workTime._1.getTime
            else
              ts=start.getTime

            if(end.before(workTime._2))
              te=end.getTime
            else
              te=workTime._2.getTime

            Intersection=(te-ts).toDouble
            val t1=(end.getTime-start.getTime).toDouble


            if(Intersection/t1>0.5&&Intersection>threeH.toLong)
              {
                attri="work"
              }

            else if((Intersection/t1)<=0.5&&(t1-
              Intersection)>twoH.toLong)
              {
                attri="home"
              }

        }
      else
        {
          Intersection=end.getTime-start.getTime
          if(Intersection>twoH.toLong)
            attri="home"
        }
      attri
  }

  case class sPoint(id:String,lng:Double,lat:Double,sDate:Date,eDate:Date,kind:String)


  def parse(line:String)={
      val pieces=line.split(",")
      val id=pieces(0)
      val lng=pieces(1).toDouble
      val lat=pieces(2).toDouble
      val sDate=new Date(pieces(3))
      val eDate=new Date(pieces(4))
      val kind=pieces(5)
      sPoint(id,lng,lat,sDate,eDate,kind)
  }



  def judgebh(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date, String)]))={
      val se=line._2.map(x=>x._6).toSeq.size
      var res=false
     if(se<2)
       res
      else
       {
          val ele=line._2
          var count=0
          for(x<-ele)
          {
            if(x._2>116.335009&&x._2<116.346805&&x._3>39.97529&&x._3<39.9851)
               count=count+1
          }
         if(count>0)
           res=true
       }
      res
  }

  /*
    判断北航职住分类
    hw：职住都在北航
    w：职在北航
    h：住在北航
   */
  def div(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date, String)]))={
     var res=""
     val ele=line._2
     val all=ele.size
     var countHW=0
     var countW=0
     var countH=0
     for(x<-ele)
       {
           if(x._2>116.335009&&x._2<116.346805&&x._3>39.97529&&x._3<39.9851)
           {
                countHW=countHW+1
                if(x._6.equals("home"))
                  countH=countH+1
                else
                  countW=countW+1
            }
       }

    if(countHW==all)
      res="hw"
    else if(countH>0)
      res="h"
    else if(countW>0)
      res="w"


    res
  }

  /*
   移动距离
   */
  def dis(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={
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
                calc=calcDis(next._2,next._3,cur._2,cur._3)
                res=res+calc
            }
         (line._1,res)
       }

  }

  /*
    出行时间
   */
  def moveTime(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)])): (String, Long) ={
     var res=0L
     val ele=line._2.toArray.sortBy(x=>x._4)
     val length=ele.length
    if(length<2)
      (line._1,res)
    else
      {
          for(i<-0 until length-1)
            {
                 val stime=ele(i)._5.getTime
                 val etime=ele(i+1)._4.getTime
                 res=res+(etime-stime)
            }
        (line._1,res)
      }


  }


  /*
    工作时间
   */

  def workTime(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={
      var resWork=0L
      var resHome=0L
      for(s<-line._2)
        {
            if(s._6.equals("work"))
              {
                  resWork=resWork+(s._5.getTime-s._4.getTime)
              }

          if(s._6.equals("home"))
          {
            resHome=resHome+(s._5.getTime-s._4.getTime)
          }
        }

       (line._1,resWork,resHome)
  }


  /*
    提取特征
   */
  def feature(line:(String, (String,Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)])))={
     val kind=line._2._1
     val xl=line._2._2.toArray.sortBy(x=>x._4).toIterable
     var homeWork=0
    if(kind.equals("hw"))
      homeWork=1
    else if(kind.equals("h"))
      homeWork=2
    else
      homeWork=3

     val para=(line._1,line._2._2)
     val disRes=dis(para)._2
     val moveTimeRes=moveTime(para)._2
     val moveCount=xl.size
     val workTimeRes=workTime(para)._2
    (homeWork,disRes,moveTimeRes,moveCount,workTimeRes)
  }



  /*
    划分区域
   */
   def divZone(lngR:Double,latR:Double,lngL:Double,latL:Double,paraLng:Double,paraLat:Double)={
       if(paraLng>lngL&&paraLng<lngR&&paraLat>latL&&paraLat<latR)
          true
       else
          false
  }

   /*
     过滤出门时间，回家时间不好的用户
    */
   def judgeInOut(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={
     val ele=line._2.toArray.sortBy(x=>x._4)
     var res=false

     if(ele.length<2)
       res
     else
       {
           if(ele(0)._6.equals("home")&&ele(ele.length-1)._6.equals("home"))
             res=true
       }

     res
   }


  /*
    出门时间，回家时间
   */
    def inOutTime(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={
        val ele=line._2.toArray.sortBy(x=>x._4)
        var outTime=0.0
        var inTime=0.0
        var mark=true


        if(ele.length==1)
          (0.0,0.0)
        else
           {
                outTime=ele(0)._5.getHours
                var j=ele.length-1

                while(j>0&&mark)
                  {
                       if(calcDis(ele(j)._2,ele(j)._3,ele(0)._2,ele(0)._3)>300)
                         {
                             if(j==ele.length-1)
                              {
                                inTime=ele(j)._5.getHours

                              }
                              else
                               {
                                   inTime=ele(j+1)._4.getHours
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

  def routeSimlar(line1:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]),
                  line2:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={


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
          val stime=l._4.getHours*6+l._4.getMinutes
          val etime=l._5.getHours*6+l._5.getMinutes
          val lng=l._2
          val lat=l._3
          for(index<- stime to etime)
            {
              ele1(index)(0)=lng
              ele1(index)(1)=lat
            }
      }

    for(l<-line2._2)
    {
      val stime=l._4.getHours*6+l._4.getMinutes
      val etime=l._5.getHours*6+l._5.getMinutes
      val lng=l._2
      val lat=l._3
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





  def calcRate(line:(String, Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={
       var allTime=0
       var dyc=0.0
       var xzl=0.0
       var oth=0.0

    for(s<-line._2)
        {
              val time=s._5.getHours-s._4.getHours
              allTime=allTime+time
        }
    for(s<-line._2)
      {
          val temp=s._5.getHours-s._4.getHours
          if(s._2>116.33531&&s._2<116.33977&&s._3>39.97503&&s._3<39.97762)
            dyc=dyc+temp
          else if(s._2>116.3440&&s._2<116.3474&&s._3>39.9779&&s._3<39.9801)
            xzl=xzl+temp
          else
            oth=oth+temp
      }


    (dyc/allTime,xzl/allTime,oth/allTime)
  }




   def resConv(arr:Array[Double])={
     val res=new Array[Double](12)
     var all=0.0

     for(i<- 0 to 8)
          {
               if(arr(i)==1.0)
                 {
                  res(0)=res(0)+1
                   all=all+1
                 }
               else if(arr(i)==2.0)
                 {
                   res(1)=res(1)+1
                   all=all+1
                 }
               else if(arr(i)==3.0)
                 {
                   res(2)=res(2)+1
                   all=all+1
                 }
          }


     for(i<- 9 to 12)
     {
       if(arr(i)==1.0)
       {
         res(3)=res(3)+1
         all=all+1
       }
       else if(arr(i)==2.0)
       {
         res(4)=res(4)+1
         all=all+1
       }
       else if(arr(i)==3.0)
       {
         res(5)=res(5)+1
         all=all+1
       }
     }

     for(i<- 13 to 18)
     {
       if(arr(i)==1.0)
       {
         res(6)=res(6)+1
         all=all+1
       }
       else if(arr(i)==2.0)
       {
         res(7)=res(7)+1
         all=all+1
       }
       else if(arr(i)==3.0)
       {
         res(8)=res(8)+1
         all=all+1
       }
     }

     for(i<- 19 to 23)
     {
       if(arr(i)==1.0)
       {
         res(9)=res(9)+1
         all=all+1
       }
       else if(arr(i)==2.0)
       {
         res(10)=res(10)+1
         all=all+1
       }
       else if(arr(i)==3.0)
       {
         res(11)=res(11)+1
         all=all+1
       }
     }

     for(i<- 0 to 2)
       {
          res(i)=res(i)/9
       }

     for(i<- 3 to 5)
     {
       res(i)=res(i)/4
     }

     for(i<- 6 to 8)
     {
       res(i)=res(i)/6
     }

     for(i<- 9 to 11)
     {
       res(i)=res(i)/5
     }

     Vectors.dense(res)
   }

// val hwc=new Array[Double](4)
//
//  val data=hwc.map{
//    x=>
//      val inOut=inOutTime(x)
//      val rate=calcRate(x)
//      val ele=Array(inOut._1,inOut._2,rate._1,rate._2,rate._3)
//      Vectors.dense(ele)
//  }.cache()

//  val dataArray=data.map(_.toArray)
//  val numCols=dataArray.first().length
//  val n=dataArray.count()
//  val sums=dataArray.reduce((a,b)=>a.zip(b).map(t=>t._1+t._2))
//  val sumSquares=dataArray.fold(new Array[Double](numCols))(
//    (a,b)=>a.zip(b).map(t=>t._1+t._2*t._2)
//  )
//  val stdevs = sumSquares.zip(sums).map{
//    case(sumSq,sum)=>math.sqrt(n*sumSq-sum*sum)/n
//  }
//  val means = sums.map(_/n)
//
//  def normalize(datum:Vector)={
//    val normalizedArray = (datum.toArray,means,stdevs).zipped.map(
//      (value,mean,stdev)=>
//         if(stdev<=0)(value-mean)else (value-mean)/stdev
//
//    )
//    Vectors.dense(normalizedArray)
//  }

  def main(args: Array[String]): Unit = {

   // val conf=new SparkConf().setAppName("test")
  //  val sc=new SparkContext(conf)

   // var rdd1=sc.textFile("hdfs://bigdata01:9000/home/xw/test/2014moblieData/*").filter(x=>judgeData(x))

    // var rdd2=rdd1.map(line=>(line.split(",")(0),line)).groupByKey()
    //   var rdd3=rdd2.filter(x=>judgeUser(x))
   // val test=Source.fromFile("E:\\study\\毕设\\聚类测试\\5.csv").getLines().toArray.map(x=>parse(x))
  //  val time=30*60*1000
   // val out=tDbscanAndJudgeAttriTest(("123",test),1000.0,time,2)
   // for(s<-out)
    //  {
     //     println(s._1+","+s._2+","+s._3+","+s._4+","+s._5(0))

     // }
   // out._2.foreach(println)
   // out._3.foreach(println)
   // out._2.map(x=>x.lng+","+x.lat+","+x.dStart+","+x.dEnd+","+"Stop").foreach(println)
   // out._3.map(x=>x.lng+","+x.lat+","+x.dmove+","+"Move").foreach(println)


   /* for(i<-3 to 7)
      {
            var rdd=rdd1.filter(x=>(x.split(",")(2)).charAt(7)-48==i)
            var rdd2=rdd.map(x=>(x.split(",")(0),x)).groupByKey().map(x=>sortByTime(x)).
              filter(x=>judgeUserV2(x)).map(x=>deleteShakeV3(x)).map{x=>
                val res=new ListBuffer[String]
                for(data<-x._2)
                  {
                   // id,time,lng,lat
                    res+=x._1+","+data._1+","+data._2+","+data._3
                  }
                res
            }.flatMap(x=>x).saveAsTextFile("hdfs://bigdata01:9000/home/xw/test/preProc/"+i)

      }*/

    val s="Thu Nov 06 21:06:51 CST 2014"

     val tmp=new Date(s.replace("CST",""))
    val o=new Date(tmp.getYear,tmp.getMonth,tmp.getDate,7,0,0)
    if(tmp.after(o))
      print(tmp)







  }
}
