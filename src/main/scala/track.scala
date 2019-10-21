import java.io.{File, FileWriter, PrintWriter}
import java.util.Date

import test.{calcDis, cellData}

import scala.collection.mutable
import scala.io.Source

import org.apache.spark

object JZLocationConverter {
  private def LAT_OFFSET_0(x: Double, y: Double) = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.sqrt(Math.abs(x))

  private def LAT_OFFSET_1(x: Double, y: Double) = (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0

  private def LAT_OFFSET_2(x: Double, y: Double) = (20.0 * Math.sin(y * Math.PI) + 40.0 * Math.sin(y / 3.0 * Math.PI)) * 2.0 / 3.0

  private def LAT_OFFSET_3(x: Double, y: Double) = (160.0 * Math.sin(y / 12.0 * Math.PI) + 320 * Math.sin(y * Math.PI / 30.0)) * 2.0 / 3.0

  private def LON_OFFSET_0(x: Double, y: Double) = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.sqrt(Math.abs(x))

  private def LON_OFFSET_1(x: Double, y: Double) = (20.0 * Math.sin(6.0 * x * Math.PI) + 20.0 * Math.sin(2.0 * x * Math.PI)) * 2.0 / 3.0

  private def LON_OFFSET_2(x: Double, y: Double) = (20.0 * Math.sin(x * Math.PI) + 40.0 * Math.sin(x / 3.0 * Math.PI)) * 2.0 / 3.0

  private def LON_OFFSET_3(x: Double, y: Double) = (150.0 * Math.sin(x / 12.0 * Math.PI) + 300.0 * Math.sin(x / 30.0 * Math.PI)) * 2.0 / 3.0

  private val RANGE_LON_MAX = 137.8347
  private val RANGE_LON_MIN = 72.004
  private val RANGE_LAT_MAX = 55.8271
  private val RANGE_LAT_MIN = 0.8293
  private val jzA = 6378245.0
  private val jzEE = 0.00669342162296594323

  def transformLat(x: Double, y: Double): Double = {
    var ret = LAT_OFFSET_0(x, y)
    ret += LAT_OFFSET_1(x, y)
    ret += LAT_OFFSET_2(x, y)
    ret += LAT_OFFSET_3(x, y)
    ret
  }

  def transformLon(x: Double, y: Double): Double = {
    var ret = LON_OFFSET_0(x, y)
    ret += LON_OFFSET_1(x, y)
    ret += LON_OFFSET_2(x, y)
    ret += LON_OFFSET_3(x, y)
    ret
  }

  def outOfChina(lat: Double, lon: Double): Boolean = {
    if (lon < RANGE_LON_MIN || lon > RANGE_LON_MAX) return true
    if (lat < RANGE_LAT_MIN || lat > RANGE_LAT_MAX) return true
    false
  }

  def gcj02Encrypt(ggLat: Double, ggLon: Double): JZLocationConverter.LatLng = {
    val resPoint = new JZLocationConverter.LatLng
    var mgLat = .0
    var mgLon = .0
    if (outOfChina(ggLat, ggLon)) {
      resPoint.latitude = ggLat
      resPoint.longitude = ggLon
      return resPoint
    }
    var dLat = transformLat(ggLon - 105.0, ggLat - 35.0)
    var dLon = transformLon(ggLon - 105.0, ggLat - 35.0)
    val radLat = ggLat / 180.0 * Math.PI
    var magic = Math.sin(radLat)
    magic = 1 - jzEE * magic * magic
    val sqrtMagic = Math.sqrt(magic)
    dLat = (dLat * 180.0) / ((jzA * (1 - jzEE)) / (magic * sqrtMagic) * Math.PI)
    dLon = (dLon * 180.0) / (jzA / sqrtMagic * Math.cos(radLat) * Math.PI)
    mgLat = ggLat + dLat
    mgLon = ggLon + dLon
    resPoint.latitude = mgLat
    resPoint.longitude = mgLon
    resPoint
  }

  def gcj02Decrypt(gjLat: Double, gjLon: Double): JZLocationConverter.LatLng = {
    val gPt = gcj02Encrypt(gjLat, gjLon)
    val dLon = gPt.longitude - gjLon
    val dLat = gPt.latitude - gjLat
    val pt = new JZLocationConverter.LatLng
    pt.latitude = gjLat - dLat
    pt.longitude = gjLon - dLon
    pt
  }

  def bd09Decrypt(bdLat: Double, bdLon: Double): JZLocationConverter.LatLng = {
    val gcjPt = new JZLocationConverter.LatLng
    val x = bdLon - 0.0065
    val y = bdLat - 0.006
    val z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * Math.PI)
    val theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * Math.PI)
    gcjPt.longitude = z * Math.cos(theta)
    gcjPt.latitude = z * Math.sin(theta)
    gcjPt
  }

  def bd09Encrypt(ggLat: Double, ggLon: Double): JZLocationConverter.LatLng = {
    val bdPt = new JZLocationConverter.LatLng
    val x = ggLon
    val y = ggLat
    val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * Math.PI)
    val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * Math.PI)
    bdPt.longitude = z * Math.cos(theta) + 0.0065
    bdPt.latitude = z * Math.sin(theta) + 0.006
    bdPt
  }

  /**
    * @param location 世界标准地理坐标(WGS-84)
    * @return 中国国测局地理坐标（GCJ-02）<火星坐标>
    * @brief 世界标准地理坐标(WGS-84) 转换成 中国国测局地理坐标（GCJ-02）<火星坐标>
    *
    *        ####只在中国大陆的范围的坐标有效，以外直接返回世界标准坐标
    */
  def wgs84ToGcj02(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = gcj02Encrypt(location.latitude, location.longitude)

  /**
    * @param location 中国国测局地理坐标（GCJ-02）
    * @return 世界标准地理坐标（WGS-84）
    * @brief 中国国测局地理坐标（GCJ-02） 转换成 世界标准地理坐标（WGS-84）
    *
    *        ####此接口有1－2米左右的误差，需要精确定位情景慎用
    */
  def gcj02ToWgs84(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = gcj02Decrypt(location.latitude, location.longitude)

  /**
    * @param location 世界标准地理坐标(WGS-84)
    * @return 百度地理坐标（BD-09)
    * @brief 世界标准地理坐标(WGS-84) 转换成 百度地理坐标（BD-09)
    */
  def wgs84ToBd09(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = {
    val gcj02Pt = gcj02Encrypt(location.latitude, location.longitude)
    bd09Encrypt(gcj02Pt.latitude, gcj02Pt.longitude)
  }

  /**
    * @param location 中国国测局地理坐标（GCJ-02）<火星坐标>
    * @return 百度地理坐标（BD-09)
    * @brief 中国国测局地理坐标（GCJ-02）<火星坐标> 转换成 百度地理坐标（BD-09)
    */
  def gcj02ToBd09(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = bd09Encrypt(location.latitude, location.longitude)

  /**
    * @param location 百度地理坐标（BD-09)
    * @return 中国国测局地理坐标（GCJ-02）<火星坐标>
    * @brief 百度地理坐标（BD-09) 转换成 中国国测局地理坐标（GCJ-02）<火星坐标>
    */
  def bd09ToGcj02(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = bd09Decrypt(location.latitude, location.longitude)

  /**
    * @param location 百度地理坐标（BD-09)
    * @return 世界标准地理坐标（WGS-84）
    * @brief 百度地理坐标（BD-09) 转换成 世界标准地理坐标（WGS-84）
    *
    *        ####此接口有1－2米左右的误差，需要精确定位情景慎用
    */
  def bd09ToWgs84(location: JZLocationConverter.LatLng): JZLocationConverter.LatLng = {
    val gcj02 = bd09ToGcj02(location)
    gcj02Decrypt(gcj02.latitude, gcj02.longitude)
  }

  class LatLng() {
    var latitude = .0
    var longitude = .0

    def this(latitude: Double, longitude: Double) {
      this()
      this.latitude = latitude
      this.longitude = longitude
    }

    def getLatitude: Double = latitude

    def setLatitude(latitude: Double): Unit = {
      this.latitude = latitude
    }

    def getLongitude: Double = longitude

    def setLongitude(longitude: Double): Unit = {
      this.longitude = longitude
    }
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




  def retrieve_neighborsT(index_center:Int, df:Array[(Int,Date,Double,Double,Array[Int])],
                          spatial_threshold:Double)={

    val center_point=df.filter(x=>x._1==index_center)(0)

    val df1=df.filter(x=>calcDis(center_point._3,center_point._4,x._3,x._4)<=spatial_threshold)

    val mid=df1.filter(x=>x._1!=index_center)

    mid
  }


  def tDbscanAndJudgeAttri(line:(String,Iterable[cellData]),spatial_threshold:Double,
                           min_neighbors:Int)={

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
        var neighbor = retrieve_neighborsT(data._1, df, spatial_threshold)
        if(neighbor.length<min_neighbors)
          data._5(0)=0
        else{
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
              spatial_threshold
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


    (line._1,df)

  }
  def main(args: Array[String]): Unit = {

 //     val con=Source.fromFile("E:\\study\\毕设\\测试数据\\notStu.csv")
  //    val writer=new PrintWriter(new File("E:\\study\\毕设\\测试数据\\notStuC.csv" ))

    //"日照":[119.46,35.42],
   /* for(line<-con.getLines())
      {
           val ele=line.split(",")
           val lng=ele(1).toDouble
           val lat=ele(2).toDouble
           val coor=gcj02ToBd09(new LatLng(lat,lng))
           writer.write("\""+ele(0)+"\""+":["+coor.longitude+","+coor.latitude+"],")
           writer.write("\n")
      }
   writer.close()*/
    //{name: "青岛", value: 18},
 /*   for(line<-con.getLines())
    {
      val ele=line.split(",")
      writer.write("{name: "+"\""+ele(0)+"\","+"value:"+9+"},")
      writer.write("\n")
    }
    writer.close()*/


//    val res=new scala.collection.mutable.ArrayBuffer[cellData]
//    for(line<-con.getLines())
//    {
//      val ele=line.split(",")
//      val lng=ele(1).toDouble
//      val lat=ele(2).toDouble
//      val date=new Date()
//
//      if(lng>116.2855&&lng<116.37736&&lat>39.9479&&lat<40.0222)
//         res += cellData(ele(0),date,lng,lat)
//    }
//
//       val para=("1",res.toIterable)
//
//
//      val cluster=tDbscanAndJudgeAttri(para,150.0,2)
//      val df=cluster._2
//      val stop=df.groupBy(x=>x._5(0)).filter(x=>x._1!=0).map{x=>
//
//
//      var clng=0.0
//      var clat=0.0
//      val l=x._2
//      for(y<-l)
//      {
//        clng+=y._3
//        clat+=y._4
//      }
//        (x._1,clng/l.length,clat/l.length)
//    }
//
//    //"日照":[119.46,35.42],
//        for(s<-stop)
//          {
//              writer.write(s._1+","+s._2+","+s._3)
//              writer.write("\n")
//          }
//
//    writer.close()

    /*   val minus14=14*60*60*1000L
      val res=new scala.collection.mutable.ArrayBuffer[(Int,Date,Double,Double,Array[Int])]
       for(line<-con.getLines())
         {
             val ele=line.split(",")
             val lng=ele(1).toDouble
             val lat=ele(2).toDouble
             val res=wgs84ToGcj02(new LatLng(lat,lng))
             val startDate=new Date(new Date(ele(3).replace("CST","")).getTime-minus14)
             val endDate=new Date(new Date(ele(4).replace("CST","")).getTime-minus14)
             writer.write(ele(0)+","+res.longitude+","+res.latitude+","+startDate+","+endDate+","+ele(5))
             writer.write("\n")
         }

     writer.close()*/


//    for (line <- con.getLines()) {
//      val ele = line.split(",")
//      val kind = ele(8)
//
//      if (kind.equals("unknown")) {
//        val lng = ele(5).toDouble
//        val lat = ele(6).toDouble
//        val res = wgs84ToGcj02(new LatLng(lat, lng))
//
//        writer.write(ele(0) + "," + res.longitude + "," + res.latitude)
//        writer.write("\n")
//      }
//    }
//
//    writer.close()

 /*   for(line<-con.getLines())
    {
      val ele=line.split(",")
      val lng=ele(0).toDouble
      val lat=ele(1).toDouble
      val res=wgs84ToGcj02(new LatLng(lat,lng))

      //116.358226,39.990697   116.336614,39.973845

      if(res.longitude>116.336614&&res.longitude<116.358226&&res.latitude>39.973845&&res.latitude<39.990697)
      {
        writer.write(res.longitude+","+res.latitude)
        writer.write("\n")
      }


    }

    writer.close()*/

    val work1Lng=116.355905
    val work1Lat=39.986696

    val resWork1=gcj02ToWgs84(new LatLng(work1Lat,work1Lng))


    val work2RLng=116.355542
    val work2RLat=39.979102

    val resRWork2=gcj02ToWgs84(new LatLng(work2RLat,work2RLng))


    val work2LLng=116.346875
    val work2LLat=39.976046

    val resLWork2=gcj02ToWgs84(new LatLng(work2LLat,work2LLng))

    println("resWork1 :"+resWork1.longitude+","+resWork1.latitude)
    println("resRWork2 :"+resRWork2.longitude+","+resRWork2.latitude)
    println("resLWork2 :"+resLWork2.longitude+","+resLWork2.latitude)


  }

}