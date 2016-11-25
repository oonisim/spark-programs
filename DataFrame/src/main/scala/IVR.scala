import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

object IVR {
  val INPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/ivr_segments.csv"
  val OUTPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/ivr_segment.out.csv"
  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSS"
  val CSV_SEPARATOR = ","
  val FIELD_SEPARATOR = "~"

  val timestampFormatter: SimpleDateFormat = {
    new SimpleDateFormat(TIMESTAMP_FORMAT)
  }
  def advTimestamp(t: Timestamp, seconds: Int): Timestamp = {
    new Timestamp(t.getTime() + seconds * 1000);
  }
  def getTimestamp(s: Any): Option[java.sql.Timestamp] = {
    val format = timestampFormatter
    if (s.toString() == "") {
      println("Timestamp field is empty")
      None
    } else {
      val d = format.parse(s.toString());
      val t = new Timestamp(d.getTime());
      return Some(t)
    }
  }

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("IVR")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc = spark.sparkContext
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._
    val ivrInput = sc.textFile(INPUT_FILE).map(in => in.split(CSV_SEPARATOR))
    val ivrRec = for {
      input <- ivrInput if (input(IVRInput.STYPE).toUpperCase != "DUP")
      stamp <- List(getTimestamp(input(IVRInput.TIMESTAMP))) if (stamp != None)
    } yield {
      val start = stamp.getOrElse(new Timestamp(0))
      val durations = input(IVRInput.MENU_DURATION).split(FIELD_SEPARATOR)
      val total = durations.foldLeft(0)((total, duration) => total + duration.toInt)
      val end = advTimestamp(start, total)

      IVRSegment(
        input(IVRInput.TIMESTAMP),
        timestampFormatter.format(end),
        total,
        input(IVRInput.ACCT_ID),
        input(IVRInput.CALL_ID),
        input(IVRInput.NUMBER_DIALED),
        durations.length)
    }

    val ivrDF = ivrRec.toDF()
    //ivrDF.repartition(1).write.csv(OUTPUT_FILE)
    ivrDF.repartition(1).write
    //ivrDF.collect.foreach(println)

    ivrDF.registerTempTable("ivrp")
  }
}