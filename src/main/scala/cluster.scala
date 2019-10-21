import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._

/*
  def convert(line:(String,Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={

    val ele=new Array[Double](72)
    for(s<-line._2)
    {
      val shour=s._4.getHours
      val ehour=s._5.getHours
      if(s._2>116.33531&&s._2<116.33977&&s._3>39.97503&&s._3<39.97762)
      {
        for(l<-shour*3 to ehour*3 by 3)
        {
          ele(l)=1.0
          ele(l+1)=0.0
          ele(l+2)=0.0
        }
      }
      else if(s._2>116.3440&&s._2<116.3474&&s._3>39.9779&&s._3<39.9801)
      {
        for(l<-shour*3 to ehour*3 by 3)
        {
          ele(l)=0.0
          ele(l+1)=1.0
          ele(l+2)=0.0
        }
      }
      else
      {
        for(l<-shour*3 to ehour*3 by 3)
        {
          ele(l)=0.0
          ele(l+1)=0.0
          ele(l+2)=1.0
        }

      }

    }
   val t= Vectors.dense(ele)
    (line._1,Vectors.dense(ele))


  }


  def distance(a: Vector, b: Vector)=math.sqrt(a.toArray.zip(b.toArray).map(p=>p._1-p._2).map(d=>d*d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel)={


   val cluster=model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid,datum)

  }

  def clusteringScore(data: RDD[Vector], k:Int)={
     val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0)
    val model = kmeans.run(data)
    data.map(datum=> distToCentroid(datum,model)).mean()
  }



def convert(line:(String,Iterable[(String, Double, Double, java.util.Date, java.util.Date,String)]))={

  val ele=new Array[Double](96)
  for(s<-line._2)
  {
    val shour=s._4.getHours
    val ehour=s._5.getHours
    if(s._2>116.33531&&s._2<116.33977&&s._3>39.97503&&s._3<39.97762)
    {
      for(l<-shour to ehour)
      {
        ele(l)=1
      }
    }
    else if(s._2>116.3440&&s._2<116.3474&&s._3>39.9779&&s._3<39.9801)
    {
      for(l<-shour to ehour)
      {
        ele(l)=2
      }
    }
    else
    {
      for(l<-shour to ehour)
      {
        ele(l)=3
      }

    }

  }

  (line._1,Vectors.dense(ele))

}*/


