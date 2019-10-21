import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object cs {

  def main(args: Array[String]): Unit = {
     val d="20190601034000"
     val format=new SimpleDateFormat("yyyyMMddHHmmss")
     val date=format.parse(d)
    val calendar: Calendar = Calendar.getInstance
      calendar.setTime(date)

    print(calendar.get(Calendar.DAY_OF_MONTH)+","+calendar.get(Calendar.HOUR_OF_DAY))
  }
}
