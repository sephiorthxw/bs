import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable

class coordinate(lng:String, lat:String) extends Serializable {
    val  Lng = lng
    val  Lat = lat

  def canEqual(other: Any): Boolean = other.isInstanceOf[coordinate]

  override def equals(other: Any): Boolean = other match {
    case that: coordinate =>
      (that canEqual this) &&
        Lng == that.Lng &&
        Lat == that.Lat
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(Lng, Lat)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def show(x: Option[Int]) = x match {
    case Some(s) => s
    case None => -1
  }
}



