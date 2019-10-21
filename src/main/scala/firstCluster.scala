import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import java.util.{Calendar, Date}

import JZLocationConverter.{LatLng,wgs84ToGcj02}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib

import scala.collection.mutable
import scala.io.Source
import scala.util.control.Breaks




object firstCluster {



  var calendar = Calendar.getInstance();
  val format=new SimpleDateFormat("yyyyMMddHHmmss")
  /**
    * 天级别（短期）停留点
    * @param plng
    * @param plat
    * @param pdStart
    * @param pdEnd
    * @param pattri
    */
  class stopPoint(plng:Double,plat:Double,pdStart:Date,pdEnd:Date,pattri:String) extends Serializable {
    var lng=plng
    var lat=plat
    var dStart=pdStart
    var dEnd=pdEnd
    var attri=pattri

    override def toString: String = {

      lng+","+lat+","+dStart+","+dEnd+","+attri
    }
  }

  /**
    * 抽象对象代表一条记录所需要的信息
    * @param id
    * @param sdate
    * @param edate
    * @param lng
    * @param lat
    */
  case class cellData(id:String,sdate: Date,edate: Date ,lng:Double,lat:Double) {
    override def toString: String = {
      id + "," + sdate.toString+ "," + edate.toString + "," + lng + "," + lat
    }
  }

  /**
    * 移动点，目前我们不是很关注这些但是留着以后用
    * @param lng
    * @param lat
    * @param smove
    * @param emove
    */
  case class movePoint(lng:Double,lat:Double,smove:Date, emove:Date){
    override def toString: String = {
      lng+","+lat+","+ smove.toString() + "," + emove.toString()
    }
  }

  /**
    * 读取数据并映射成（id, cellData）的格式
    * @param line
    * @return
    */
  def parseData(line: String): (String, cellData) = {
    var items = line.split(",")
    var uuid = items(0)
    var format = new SimpleDateFormat("yyyyMMddHHmmss")
    var sdate = format.parse(items(1))
    var edate = format.parse(items(2))
    var lng = items(3).toDouble
    var lat = items(4).toDouble

    (uuid,cellData(uuid,sdate,edate,lng,lat))
  }

  def parseDataForTest(line:String):(String,cellData)={
    //第一个'['字符的位置
    val firstI=line.indexOf('[')
    //'['字符后第一个逗号的位置
    val firstD=line.indexOf(',',firstI)
    //起始时间
    val stimeString=line.substring(firstI+6,firstD).toLong
    val stime = new Date(stimeString)

    val secondI=line.indexOf('[',firstD)
    val thirdI=line.indexOf('[',secondI+1)
    val secondD=line.indexOf(',',thirdI)
    val etimeString=line.substring(thirdI+6,secondD).toLong

    val etime = new Date(etimeString)
    //第一个']'
    val lastO=line.lastIndexOf(']')


    val ele=line.substring(lastO+2).split(",")


    val startD=line.indexOf(',')
    val id=line.substring(0,startD)

    (id,cellData(id,stime,etime,ele(0).toDouble,ele(1).toDouble))
  }

  /**
    * 去除错误数据
    *
    * @param x
    * @return
    */
  def judgeData(x: String): Boolean = {

    try {
      var start_date = format.parse(x.split(",")(1))
      var end_date = format.parse(x.split(",")(2))
      val lng = x.split(",")(3).toDouble
      val lat = x.split(",")(4).toDouble
      if (lat<39.4333||lat>41.05||lng<115.41667||lng>117.5) {
        return false
      }
      return true;
    }
    catch {
      case e:Exception=>{
        return false
      }
    }
  }

  /**
    * groupby 用户id, 提取日大于n个小时的用户
    * @param data
    * @param n (hour)
    * @return
    */
  def activeData(data:(String, Iterable[cellData]), n:Int) :Boolean = {
    var cellDatas = data._2
    var DaySet = mutable.Set[Int]()
    var activeTime = 0L
    for (data <- cellDatas) {
      activeTime += (data.edate.getTime - data.sdate.getTime)
    }
    activeTime >= n*3600*1000

  }

  /**
    * 距离计算
    * 单位：米
    * @param lng1
    * @param lat1
    * @param lng2
    * @param lat2
    * @return
    */
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

  /**
    * 为TDBSCAN算法找邻居
    *
    * @param index_center
    * @param df
    * @param spatial_threshold
    * @param temporal_threshold
  × @param min_neighbors : 包含自己的最小邻居数目
    * @return
    */
  def retrieve_neighborsT(index_center:Int, df:Array[(Int,Date,Date,Double,Double,Array[Int])],
                          spatial_threshold:Double, temporal_threshold:Long, min_neighbors:Int)={
    val res=new scala.collection.mutable.ArrayBuffer[(Int,Date,Date,Double,Double,Array[Int])]
    val empty=new scala.collection.mutable.ArrayBuffer[(Int,Date,Date,Double,Double,Array[Int])]
    var forward=index_center
    var backward=index_center
    val loop1=new Breaks
    val loop2=new Breaks
    loop1.breakable{
      while(backward>0)
      {
        backward-=1
        if(calcDis(df(backward)._4,df(backward)._5,df(index_center)._4,df(index_center)._5)<=spatial_threshold)
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
        if(calcDis(df(forward)._4,df(forward)._5,df(index_center)._4,df(index_center)._5)<=spatial_threshold)
        {
          res+=df(forward)
        }
        else
          loop2.break()
      }
    }
    res+=df(index_center)
    res.sortBy(x=>x._2)
    if(res(res.length-1)._3.getTime-res(0)._2.getTime<temporal_threshold)
      empty
    if( res.length < min_neighbors && (res.map( x => x._3.getTime()-x._2.getTime()).sum)/(1000*10*60) < 3) // 不关心邻居的个数 但是关心邻居的时长（上面）
      empty
    else
      res.filter(x=>x._1!=index_center)
  }

  /**
    * TDBSCAN 算法
    * @param line
    * @param spatial_threshold
    * @param temporal_threshold
    * @param min_neighbors
    * @return
    */
  def tDbscanAndJudgeAttri(line:(String,Iterable[cellData]),spatial_threshold:Double,
                           temporal_threshold:Long,min_neighbors:Int) ={
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
      (index,x.sdate,x.edate,x.lng,x.lat,kind)
    }.toArray

    for(data<-df)
    {
      if(data._6(0) == -1) {
        var neighbor = retrieve_neighborsT(data._1, df, spatial_threshold, temporal_threshold, min_neighbors)
        //        if(neighbor.length<min_neighbors)
        //          data._6(0)=0
        if(neighbor.length < 1) {
          if (data._3.getTime() - data._2.getTime() >= temporal_threshold){
            clusterIndex += 1
            data._6(0) = clusterIndex
          }
          else {
            data._6(0) = 0;
          }

        }
        else{
          //          neighbor.remove(data._1)
          clusterIndex+=1
          data._6(0)=clusterIndex

          for(dataNeighbor<-neighbor)
          {
            dataNeighbor._6(0)=clusterIndex
            stack.push(dataNeighbor._1)
          }
          while (stack.isEmpty==false)
          {
            val cur=stack.pop()
            val newNeighbor=retrieve_neighborsT(cur,df,
              spatial_threshold,temporal_threshold,min_neighbors)
            // 找到newNeighbor
            for(s<-newNeighbor)
            {
              if(s._6(0)== -1||s._6(0)==0)
              {
                s._6(0)=clusterIndex
                stack.push(s._1)
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
    val stop=df.groupBy(x=>x._6(0)).filter(x=>x._1!=0).map{x=>
      var clng=0.0
      var clat=0.0
      val l=x._2.sortBy(t=>t._2)
      for(y<-l)
      {
        clng+=y._4
        clat+=y._5
      }
      new stopPoint(clng/l.length,clat/l.length,l(0)._2,l(l.length-1)._3,
        judgePointAttri(l(0)._2,l(l.length-1)._3))
    }.toArray.sortBy(x=>x.dStart)

    val move=df.filter(x=>x._6(0)==0).map{
      x=>
        movePoint(x._4, x._5, x._2, x._3)
    }
    //用户id,停留点集合,移动点集合
    (line._1,stop,move)
  }

  /**
    * 判断一个天级别停留点的属性，其实并没有用 只是留着判断结果日后处理
    * @param start
    * @param end
    * @return
    */
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





  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("test").setMaster("spark://master01:7077")
//    val sc = new SparkContext(conf)
//    var locationData = sc.textFile("hdfs://dcoshdfs/private_data/mengmh/jizhan20190617.txt")
//
//
//    for(i<- 6 to 6) {
//      var start = "20190" + i + "03"
//      var end = "20190" + i + "07"
//      var month = "20190" + i
//      var sdf = new SimpleDateFormat("yyyyMMdd")
//      var dstart = sdf.parse(start)
//      var dend = sdf.parse(end)
//      var calstart = Calendar.getInstance()
//      var calend = Calendar.getInstance()
//      calstart.setTime(dstart)
//      calend.setTime(dend)
//      while (calstart.before(calend)) {
//        var currentDate = calstart.getTime()
//        var currentDString = sdf.format(currentDate)
//        var inPath = "hdfs://dcoshdfs/private_data/useryjj/ImsiPath/2019/" + month + "/" + currentDString + "/*"
//        var outputPathDir = "hdfs://dcoshdfs/private_data/useryjj/1Cluster/2019/" + month + "/" + currentDString + "/"
//
//        var data = sc.textFile(inPath).filter(x => judgeData(x)).map(x => parseData(x)).groupByKey(8)
//
//        // 过滤出 有数据时长占比为20h 的用户为活跃用户
//        var active_data = data.filter( x => activeData(x,20) )
//
//        var first_results = active_data.map( x=>tDbscanAndJudgeAttri(x,300, 30*60*1000, 5)).repartition(8)
//        var sum = first_results.count()
//        var all_stop_sum = first_results.filter(x=> x._2.size > 0).count()
//        var all_move_sum = first_results.filter(x=> x._3.size > 0).count()
//
//        var allStopPoint = first_results.map(x => (x._1,x._2)).filter(x => x._2.size>0).flatMap(x => x._2 map(x._1 -> _)).map(x=>x._1+","+x._2.toString)
//        allStopPoint.saveAsTextFile(outputPathDir + "AllStop")
//        var allMovePoint = first_results.map(x => (x._1,x._3)).filter(x => x._2.size>0).flatMap(x => x._2 map(x._1 -> _)).map(x=>x._1+","+x._2.toString)
//        allMovePoint.saveAsTextFile(outputPathDir + "AllMove")
//
//        var onlyMove = first_results.filter( x=> x._2.size == 0 && x._3.size > 0).map(x => (x._1,x._3))
//        var onlyMove_count = onlyMove.count()
//        onlyMove.saveAsTextFile(outputPathDir + "OnlyMove/")
//        onlyMove.flatMap(x => x._2 map(x._1 -> _)).map(x=>x._1 + "," + x._2.toString)
//
//        calstart.add(Calendar.DAY_OF_MONTH, 1)
//      }
//    }
       val file=new File("E:\\study\\毕设\\测试数据\\bh603")
        val writer=new PrintWriter(new File("E:\\study\\毕设\\测试数据\\firstClusterObcureStu.csv" ))

        val fileList=file.listFiles()

        val ele=new ArrayBuffer[(String,cellData)]()

        for (f <- fileList) {
          val con=Source.fromFile(f)
          for (line <- con.getLines()) {
            val t = parseDataForTest(line)
            ele += t
          }
        }

       val out=ele.groupBy(x=>x._1).map{
         x=>
           val ele=new ArrayBuffer[cellData]()
           for(t<-x._2)
             ele+=t._2
           (x._1,ele.toIterable)
       }.map(x=>tDbscanAndJudgeAttri(x,300, 30*60*1000, 5))
         .flatMap(x => x._2 map(x._1 -> _))


      val inputForDiv=out.map{
        x=>
        val stop=x._2
        val stime=Calendar.getInstance()
        val etime=Calendar.getInstance()

        stime.setTime(stop.dStart)
        etime.setTime(stop.dEnd)
          (x._1,stime,etime,stop.lng,stop.lat,etime.get(Calendar.MINUTE)-stime.get(Calendar.MINUTE))
      }.groupBy(x=>x._1).map(x=>divUser(x))

    val t1=inputForDiv.filter(x=>x._3.equals("obscureStu")).map(x=>(x._1,x._2)).flatMap(x=>x._2)

    for(l<-t1)
        {

          var kind="unknown"

          if((l._4>116.33531&&l._4<116.33977&&l._5>39.97503&&l._5<39.97762))
            kind="resident1"
          else if(l._4>116.33804&&l._4<116.34204&&l._5>39.979&&l._5<39.98515)
            kind="resident2"
          else if((l._4>116.3418&&l._4<116.3497&&l._5>39.9776&&l._5<39.9854))
            kind="workTech"
          else if(l._4>116.3406&&l._4<116.3493&&l._5>39.9746&&l._5<39.9777)
            kind="workMall"


          val location=wgs84ToGcj02(new LatLng(l._5,l._4))

          writer.write(l._1+","+l._2.get(Calendar.HOUR_OF_DAY)+","+l._2.get(Calendar.MINUTE)
            +","+l._3.get(Calendar.HOUR_OF_DAY)+","+l._3.get(Calendar.MINUTE)+","+location.longitude+","+location.latitude+","+l._6+","+kind)

          writer.write("\n")
        }
        writer.close()



        var preStuCount=0
        var obscStuCount=0
        var workerCount=0
        var teacherCount=0
        var unknownCount=0

        for(l<-inputForDiv)
        {
            if(l._3.equals("preciseStu"))
              preStuCount=preStuCount+1
            else if(l._3.equals("obscureStu"))
              obscStuCount=obscStuCount+1
            else if(l._3.equals("worker"))
              workerCount=workerCount+1
            else if(l._3.equals("teacher"))
              teacherCount=teacherCount+1
            else
              unknownCount=unknownCount+1
        }

        println(preStuCount+","+obscStuCount+","+workerCount+","+teacherCount+","+unknownCount)
  }

}
