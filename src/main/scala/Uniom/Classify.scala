package Uniom

import java.util.Calendar

import Uniom.firstCluster.{cellData, divUser, parseDataForTest, tDbscanAndJudgeAttri}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object Classify {


  def getMetrics(model:org.apache.spark.mllib.tree.model.DecisionTreeModel,data:org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]):org.apache.spark.mllib.evaluation.MulticlassMetrics={
    val predictionsAndLabels = data.map(example => (model.predict(example.features),example.label
    ))
    new org.apache.spark.mllib.evaluation.MulticlassMetrics(predictionsAndLabels)
  }


  def main(args: Array[String]): Unit = {
    val conf1 = new SparkConf().setAppName("test").setMaster("spark://bigdata02:7077")
    val sc = new SparkContext(conf1)

    val ele=sc.textFile("hdfs://bigdata01:9000/home/xw/bh603").map(x=>parseDataForTest(x))

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

    val f=new feature


    val rawData=inputForDiv.filter(x=>x._3.equals("preciseStu")||x._3.equals("unknown")||x._3.equals("worker")).map{
      x=>
        val tmp=x._2.map(l=> new stopPoint(l._4,l._5,l._2.getTime,l._3.getTime,"hold"))
        (x._1,tmp,x._3)
    }.map{x=>
      val feature=f.calcOneDayFeature((x._1,x._2))
      if(x._3.equals("preciseStu"))
        (feature,0.0)
      else if(x._3.equals("worker"))
        (feature,1.0)
      else
        (feature,2.0)
    }


    val data=rawData.map{
      x=>
        val feature=Vectors.dense(x._1)
        LabeledPoint(x._2,feature)
    }

    val model = DecisionTree.trainClassifier(data,3,Map[Int,Int](),"gini",4,100)

    model.toDebugString

    val metrics= getMetrics(model,data)

    println(metrics.confusionMatrix)

    println(metrics.precision)
  }
}
