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

/**
 * --------------------------------------------------------------------------------
 * Record set of the IVR Menu which users have selected.
 *
 * [Fields]
 * start_time	"ivr_segment.event_timestamp + sum(values in field menuDuration matching up to previous menuId).
 * end_time	ivr_segment.event_timestamp + sum(values in field menuDuration matching up to current menuId). See example above
 * duration	calculated field: end_time - start_time (in seconds)
 * acct_id	pass through from input/parent
 * call_id	pass through from input/parent
 * menuId	value of current menuId (see example above)
 * menuInput	value of current menuInput (see example above)
 *
 * --------------------------------------------------------------------------------
 * The original IVR segment input records have multi-value fields.
 * Split the values and create respective records.
 *
 * [Example multi fields in an IVR segment input record]
 * menuId: 123~456~789
 * menuDuration: 10~5~4
 * menuInput: X~1~X
 *
 * [Expected respective record]
 * 14:45:17, 14:45:27, 10, [...], [...], 123,X
 * 14:45:27, 14:45:32, 5, [...], [...], 456,1
 * 14:45:32, 14:45:36, 4, [...], [...], 789,X"
 * --------------------------------------------------------------------------------
 */
case class IVRMenu(
  start_time: Timestamp,
  end_time: Timestamp,
  duration: Int,
  acct_id: String,
  call_id: String,
  menu_id: String,
  menu_input: String)

object IVRMenu {
  val START_TIME_COLUMN = 0
  val END_TIME_COLUMN = 1
  val DURATION_COLUMN = 2
  val ACCT_ID_COLUMN = 3
  val CALL_ID_COLUMN = 4
  val MENU_ID_COLUMN = 5
  val MENU_INPUT_COLUMN = 6

  /**
   * --------------------------------------------------------------------------------
   * Record set in RDD.
   * --------------------------------------------------------------------------------
   */
  def getRDD(sc: SparkContext): RDD[IVRMenu] = {
    val rdd = IVRInput.getRDD(sc)
    val menus = for {
      input <- rdd if (input.stype != "DUP")
      index <- (0 until input.menuId.split(IVRInput.MULTIFIELD_SEPARATOR).length)
    } yield {
      val start = input.event_timestamp
      if (input.menuDuration != "") {
        val ids = input.menuId.split(IVRInput.MULTIFIELD_SEPARATOR)
        val durations = input.menuDuration.split(IVRInput.MULTIFIELD_SEPARATOR)
        val inputs = input.menuInput.split(IVRInput.MULTIFIELD_SEPARATOR)
        val end = advTimestamp(start, durations(index).toInt)

        IVRMenu(
          //timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(start),
          //timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(end),
          start,
          end,
          durations(index).toInt,
          input.acct_id,
          input.call_id,
          ids(index),
          inputs(index))
      } else {
        IVRMenu(
          //timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(start),
          //timestampFormatter(IVRInput.TIMESTAMP_FORMAT).format(start),
          start,
          start,
          0,
          input.acct_id,
          input.call_id,
          "",
          "")
      }
    }
    menus
  }
  /**
   * --------------------------------------------------------------------------------
   * Save the record set as CSV in the path directory (not file).
   * --------------------------------------------------------------------------------
   */
  val OUTPUT_FILE = "./src/main/resources/IVRMenu"
  def save(sc: SparkContext, rdd: RDD[IVRMenu], path: String = OUTPUT_FILE): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF().coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv(path)
  }
}