package Uniom

import java.util.Date

import Uniom.firstCluster.cellData
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    val f=new feature

      val conf=new SparkConf().setAppName("writeData").setMaster("spark://master01:7077")
      val sc=new SparkContext(conf)
      var rdd1=sc.textFile("hdfs://dcoshdfs/private_data/useryjj/1Cluster/2019/201906/2019061*/AllStop")
      var a=3
      var rdd2=rdd1.map{
        x=>
          val ele=x.split(",")
          val id=ele(0)
          val lng=ele(1).toDouble
          val lat=ele(2).toDouble
          val sdate=new Date(ele(3).replace("CST",""))
          val edate=new Date(ele(4).replace("CST",""))
          val kind=ele(5)
          (id,new stopPoint(lng,lat,sdate,edate,kind))
      }.groupByKey().map{
        x=>
          var ele=x._2.map(x=>(x.dStart.getDay,x)).groupBy(x=>x._1).map(x=>(x._1,x._2.map(t=>t._2)))

          (x._1,ele)
      }

      val rddFirstFilter=rdd2.filter(x=>f.firstFilterWeek(x,116.3717,40.0138,116.2771,39.9434))

      rddFirstFilter.map{
        x=>
          val store=new ArrayBuffer[String]()
          for(l<-x._2)
          {
            for(t<-l._2)
              store+=x._1+","+t.toString
          }
          store
      }.flatMap(x=>x).saveAsTextFile("hdfs://dcoshdfs/private_data/useryjj/xw/bhOneWeek")
    }

}
