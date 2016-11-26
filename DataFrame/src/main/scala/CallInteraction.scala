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

case class CallInteraction(
  start_time: String,
  end_time: String,
  duration: Int,
  acct_id: String,
  call_id: String,
  primaryAgentCount: Int,
  totalAgentCount: Int)

object CallInteraction {
  val START_TIMESTAMP_COLUMN = 0
  val END_TIMESTAMP_COLUMN = 1
  val DURATION_COLUMN = 2
  val ACCT_ID_COLUMN = 3
  val CALL_ID_COLUMN = 4
  val PRIMARYAGENTCOUNT_COLUMN = 5
  val TOTALAGENTCOUNT_COLUMN = 6

  val OUTPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/CallInteratcion"

  /**
   * Set of records whose call_id only exists in Agent call segments.
   */
  def IVROnly(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sql = """
      SELECT
        ivr.start_time,
        ivr.end_time,
        unix_timestamp(ivr.end_time) - unix_timestamp(ivr.start_time) as duration,
        ivr.acct_id,
        ivr.call_id,
        'ivr_only' as type
      FROM 
        ivr
        LEFT OUTER JOIN
        agent
        ON ivr.call_id = agent.call_id
      WHERE
        agent.call_id IS NULL
    """
    val df = sqlContext.sql(sql)
    println("---------- IVR Only ----------------------------------------------------------------------")    
    df.collect.foreach(println)
    df
  }
  /**
   * Set of records whose call_id only exists in Agent call segments.
   */
  def agentOnly(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val sql = """
      SELECT
        agent.start_time,
        agent.end_time,
        unix_timestamp(agent.end_time) - unix_timestamp(agent.start_time) as duration,
        agent.acct_id,
        agent.call_id,
        'agent_only' as type
      FROM 
        ivr
        LEFT OUTER JOIN
        agent
        ON ivr.call_id = agent.call_id
      WHERE
        ivr.call_id IS NULL
    """
    val df = sqlContext.sql(sql)
    println("---------- Agent Only ----------------------------------------------------------------------")    
    df.collect.foreach(println)
    df
  }
  /**
   * Set of records whose call_id exists both in IVR segments and Agent call segments.
   */
  def IVRAndAgent(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val sql = """
      SELECT
        CASE 
          WHEN ivr.start_time < agent.start_time
            THEN ivr.start_time
          ELSE agent.start_time
        END as start_time,
        CASE 
          WHEN ivr.end_time > agent.end_time 
            THEN ivr.end_time
          ELSE agent.end_time
        END as end_time,
        CASE 
          WHEN ivr.start_time < agent.start_time AND agent.end_time < ivr.end_time 
            THEN unix_timestamp(ivr.end_time) - unix_timestamp(ivr.start_time)
          WHEN ivr.start_time < agent.start_time AND ivr.end_time < agent.end_time
            THEN unix_timestamp(agent.end_time) - unix_timestamp(ivr.start_time)
          WHEN agent.start_time < ivr.start_time AND ivr.end_time < agent.end_time
            THEN unix_timestamp(agent.end_time) - unix_timestamp(agent.start_time)
          WHEN agent.start_time < ivr.start_time AND agent.end_time < ivr.end_time
            THEN unix_timestamp(ivr.end_time) - unix_timestamp(agent.start_time)
        END as duration,
        ivr.acct_id,
        ivr.call_id,
        'ivr_and_agent' as type
      FROM 
        ivr
        INNER JOIN
        agent
        ON ivr.call_id = agent.call_id
    """
    val df = sqlContext.sql(sql)
    println("---------- Both ----------------------------------------------------------------------")
    df.collect.foreach(println)
    df
  }

  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val ivr = IVRSegment.getDF(sc)
    ivr.registerTempTable("ivr")
    val agent = AgentCall.getDF(sc)
    agent.registerTempTable("agent")

    val onlyIVR = IVROnly(sc)
    onlyIVR.registerTempTable("onlyIVR")
    val onlyAgent = agentOnly(sc)
    onlyAgent.registerTempTable("onlyAgent")
    val both = IVRAndAgent(sc)
    both.registerTempTable("both")

    val sql = """
      SELECT * FROM onlyIVR
      UNION
      SELECT * FROM onlyAgent
      UNION
      SELECT * FROM both
    """
    sqlContext.sql(sql)
  }
  def save(sc: SparkContext, df: DataFrame, path: String = OUTPUT_FILE): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv(path)
  }
}