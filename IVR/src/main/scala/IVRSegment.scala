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
  start_time: Timestamp,
  end_time: Timestamp,
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

  

  /**
   * --------------------------------------------------------------------------------
   * Set of IVR segments excluding all records of type = dup.
   * --------------------------------------------------------------------------------
   */
  def getRDD(sc: SparkContext): RDD[IVRSegment] = {
    val rdd = IVRInput.getRDD(sc)
    val segments = for {
      input <- rdd if (input.stype != "DUP")
    } yield {
      //--------------------------------------------------------------------------------
      // Get the end_time by start_time + sum(values in field menuDuration).
      // Use start_time if field menuDuration is empty.
      // Example of menuDuration: "10~5~4"
      //--------------------------------------------------------------------------------
      val start = input.event_timestamp
      val (total: Int, end: Timestamp, count:Int) = if (input.menuDuration != "") {
        val durations = input.menuDuration.split(IVRInput.MULTIFIELD_SEPARATOR)
        val total = durations.foldLeft(0)((total, duration) => total + duration.toInt)
        val end = advTimestamp(start, total)
        (total, end, durations.length)
      } else {
        (0, start, 0)
      }

      IVRSegment(
        //timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(start),
        //timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(end),
        start,
        end,
        total,
        input.acct_id,
        input.call_id,
        input.numberDialed,
        count)
    }
    segments
  }

  /**
   * --------------------------------------------------------------------------------
   * Set of IVRSergment in DataFrame
   * --------------------------------------------------------------------------------
   */
  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    getRDD(sc).toDF()
  }
  /**
   * --------------------------------------------------------------------------------
   * Save the IVRSegment DataFrame to CSV under the path directory (not file).
   * --------------------------------------------------------------------------------
   */
  //val OUTPUT_FILE = "file:///./src/main/resources/IVRSegment"
  val OUTPUT_FILE = "./src/main/resources/IVRSegment"
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