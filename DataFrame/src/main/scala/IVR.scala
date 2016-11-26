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

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IVR")
    val sc = new SparkContext(conf)

    IVRSegment.save(sc, IVRSegment.getRDD(sc))
    IVRMenu.save(sc, IVRMenu.getRDD(sc))
  }
}