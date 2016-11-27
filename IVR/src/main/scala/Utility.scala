import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import java.lang.Math

object Utility {
   def timestampFormatter(format: String) : SimpleDateFormat = {
    new SimpleDateFormat(format)
  }
  def advTimestamp(t: Timestamp, seconds: Int): Timestamp = {
    new Timestamp(t.getTime() + seconds * 1000);
  }
  def getTimestamp(s: String, format: String): Option[java.sql.Timestamp] = {
    val formatter = timestampFormatter(format)
    if (s.toString() == "") {
      println("Timestamp field is empty")
      None
    } else {
      val d = formatter.parse(s.toString());
      val t = new Timestamp(d.getTime());
      return Some(t)
    }
  }
  
}