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

case class IVRSegment(
  start_time: String,
  end_time: String,
  duration: Int,
  acct_id: String,
  call_id: String,
  numberDialed: String,
  menu_count: Int)

object IVRSegment {
  val START_TIME_COLUMN = 0
  val END_TIME_COLUMN = 1
  val DURATION_COLUMN = 2
  val ACCT_ID_COLUMN = 3
  val CALL_ID_COLUMN = 4
  val NUMBER_DIALED_COLUMN = 5
  val MENU_COUNT_COLUMN = 6

  val OUTPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/IVRSegment"

  /**
   * Set of IVR segments excluding all records of type = dup from the original "IVR Segments" data. 
   */
  def getRDD(sc: SparkContext): RDD[IVRSegment] = {
    val rdd = IVRInput.getRDD(sc)
    val segments = for {
      input <- rdd if (input.stype != "DUP")
    } yield {
      val start = input.event_timestamp
      val durations = input.menuDuration.split(IVRInput.MULTIFIELD_SEPARATOR)
      val total = durations.foldLeft(0)((total, duration) => total + duration.toInt)
      val end = advTimestamp(start, total)

      IVRSegment(
        timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(start),
        timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(end),
        total,
        input.acct_id,
        input.call_id,
        input.numberDialed,
        durations.length)
    }
    segments
  }
  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    getRDD(sc).toDF()
  }
  def save(sc: SparkContext, rdd: RDD[IVRSegment], path: String = OUTPUT_FILE): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF().coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv(path)
  }
}