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

case class AgentCallInteraction(
  start_time: String,
  end_time: String,
  duration: Int,
  acct_id: String,
  call_id: String,
  primaryAgentCount: Int,
  totalAgentCount: Int)

object AgentCallInteraction {
  val START_TIMESTAMP_COLUMN = 0
  val END_TIMESTAMP_COLUMN = 1
  val DURATION_COLUMN = 2
  val ACCT_ID_COLUMN = 3
  val CALL_ID_COLUMN = 4
  val PRIMARYAGENTCOUNT_COLUMN = 5
  val TOTALAGENTCOUNT_COLUMN = 6

  val OUTPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/interatcions"

  def binary(s: String): Int = {
    if (s.toUpperCase() == "YES") 1
    else 0
  }

  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    sqlContext.udf.register("binary", binary _)    
    val df = AgentCallSegment.getDF(sc)
    df.printSchema()
    df.registerTempTable("calls")
    
    val sql = """
      SELECT DISTINCT
        a.call_id,
        a.start_time,
        a.end_time,
        unix_timestamp(end_time) - unix_timestamp(start_time) as duration,
        acct.acct_id,
        a.primaryAgentCount,
        a.totalAgentCount
      FROM 
      (
        SELECT
          call_id,
          MIN(start_time) as start_time,
          MAX(end_time) as end_time,
          SUM(binary(prim_agent)) as primaryAgentCount,
          COUNT(*) as totalAgentCount
        FROM calls
        GROUP BY
          call_id
      ) a 
      INNER JOIN
      (
        SELECT 
          call_id, 
          first(acct_id, false) as acct_id
        FROM calls
        GROUP BY call_id
      ) acct
      ON a.call_id = acct.call_id
    """
    
    
    //val groups = sqlContext.sql("SELECT min(start_time), max(end_time), call_id FROM calls GROUP BY call_id")
    val groups = sqlContext.sql(sql)
    
    println("-----------------------------------------------------")
    groups.collect().foreach(println)
    
    groups
  }
  def save(sc: SparkContext, df: DataFrame, path: String = OUTPUT_FILE): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
}