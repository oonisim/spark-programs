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


case class IVRInput(
  event_timestamp: Timestamp,
  acct_id: String,
  call_id: String,
  numberDialed: String,
  menuId: String,
  menuDuration: String,
  menuInput: String,
  stype: String)

object IVRInput {
  val TIMESTAMP_COLUMN = 0
  val ACCT_ID_COLUMN = 1
  val CALL_ID_COLUMN = 2
  val NUMBER_DIALED_COLUMN = 3
  val MENU_ID_COLUMN = 4
  val MENU_DURATION_COLUMN = 5
  val MENU_INPUT_COLUMN = 6
  val TYPE_COLUMN = 7

  val TIMESTAMP = "event_timestamp"
  val ACCT_ID = "acct_id"
  val CALL_ID = "call_id"
  val NUMBER_DIALED = "numberDialed"
  val MENU_ID = "menuId"
  val MENU_DURATION = "menuDuration"
  val MENU_INPUT = "menuInput"
  val TYPE = "stype"

  val INPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/ivr_segments.csv"
  val OUTPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/inputs"
  val CSV_SEPARATOR = ","
  val MULTIFIELD_SEPARATOR = "~"
  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSS"

  def getRDD(sc: SparkContext): RDD[IVRInput] = {
    val records = sc.textFile(INPUT_FILE).map(line => line.split(CSV_SEPARATOR))
    val inputs = for {
        fields <- records
        stamp <- List(getTimestamp(fields(TIMESTAMP_COLUMN), TIMESTAMP_FORMAT)) if (stamp != None)
    } yield {
      IVRInput(
        stamp.get,
        fields(ACCT_ID_COLUMN),
        fields(CALL_ID_COLUMN),
        fields(NUMBER_DIALED_COLUMN),
        fields(MENU_ID_COLUMN),
        fields(MENU_DURATION_COLUMN),
        fields(MENU_INPUT_COLUMN),
        fields(TYPE_COLUMN))
    }
    inputs
  }
  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    getRDD(sc).toDF()
  }
  def save(sc: SparkContext, rdd: RDD[IVRInput], path: String = OUTPUT_FILE): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF().coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .csv(path)
  }
}