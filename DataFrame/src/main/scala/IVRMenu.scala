import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import java.lang.Math
import Utility._

case class IVRMenu(
  start_time: String,
  end_time: String,
  duration: Int,
  acct_id: String,
  call_id: String,
  menu_id: String,  
  menu_input: String)

object IVRMenu{
  val START_TIME_COLUMN = 0
  val END_TIME_COLUMN = 1
  val DURATION_COLUMN = 2
  val ACCT_ID_COLUMN = 3
  val CALL_ID_COLUMN = 4
  val MENU_ID_COLUMN = 5
  val MENU_INPUT_COLUMN = 6

  val OUTPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/menus"

  def getRDD(sc: SparkContext): RDD[IVRMenu] = {
   
    val rdd = IVRInput.getRDD(sc)
    val menus = for {
        input <- rdd if (input.stype != "DUP")
        index <- (0 until input.menuId.split(IVRInput.MULTIFIELD_SEPARATOR).length)
    } yield {
      val start = input.event_timestamp
      val ids = input.menuId.split(IVRInput.MULTIFIELD_SEPARATOR)
      val durations = input.menuDuration.split(IVRInput.MULTIFIELD_SEPARATOR)
      val inputs = input.menuInput.split(IVRInput.MULTIFIELD_SEPARATOR)

      val end = advTimestamp(start, durations(index).toInt)

      IVRMenu(
        timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(start),
        timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(end),
        durations(index).toInt,
        input.acct_id,
        input.call_id,
        ids(index),
        inputs(index))
    }
    menus
  }

  def save(sc: SparkContext, rdd: RDD[IVRMenu], path: String = OUTPUT_FILE): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF().coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .csv(path)
  }
  
}

