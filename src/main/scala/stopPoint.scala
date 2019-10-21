import java.util.Date

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
