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
 * Record set of all the calls identified by call_id.
 * Create one record for each call_id that exists in ivr_segment and/or agent_call table.
 *
 * [Reference]
 * Refer to the ER diagram of the description doucument.
 *
 * [Fields]
 * start_time	: min(ivr_segment.start_time, agent_call.start_time)
 * end_time	  : max(ivr_segment.end_time, agent_call.end_time)
 * duration	  : calculated field: end_time - start_time (in seconds)
 * acct_id    : If call has IVR Segment, use ivr_segment.acct_id. Else use agent_call.acct_id
 * call_id    : If call has IVR Segment, use ivr_segment.call_id. Else use agent_call.call_id
 * type       : Indicate if call interaction has an IVR Segment, an Agent Call Interaction or both
 *              (values: ivr_only, agent_only, ivr_and_agent)
 * --------------------------------------------------------------------------------
 */
case class CallInteraction(
  start_time: Timestamp,
  end_time: Timestamp,
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

  /**
   * --------------------------------------------------------------------------------
   * Record set of calls that only exist in IVR call segments.
   * --------------------------------------------------------------------------------
   */
  def IVROnly(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sql = """
      SELECT
        start_time,
        end_time,
        unix_timestamp(end_time) - unix_timestamp(start_time) as duration,
        acct_id,
        call_id,
        'ivr_only' as type
      FROM 
        ivr i
      WHERE NOT EXISTS (
        SELECT 1 FROM agent WHERE call_id = i.call_id
      )
    """
    val df = sqlContext.sql(sql)
    df
  }
  /**
   * --------------------------------------------------------------------------------
   * Record set of calls that only exists in Agent call segments.
   * --------------------------------------------------------------------------------
   */
  def agentOnly(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sql = """
      SELECT
        start_time,
        end_time,
        unix_timestamp(end_time) - unix_timestamp(start_time) as duration,
        acct_id,
        call_id,
        'agent_only' as type
      FROM 
        agent a
      WHERE NOT EXISTS (
        SELECT 1 FROM ivr WHERE call_id = a.call_id
      )
    """
    val df = sqlContext.sql(sql)
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
    df
  }

  /**
   * Record set in DataFrame
   */
  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val ivr = IVRSegment.getDF(sc)
    ivr.registerTempTable("ivr")
    val agent = AgentCall.getDF(sc)
    agent.registerTempTable("agent")

    //--------------------------------------------------------------------------------
    // Take the UNION of only in IVR, only in Agent, and in both.
    //--------------------------------------------------------------------------------    
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

  /**
   * --------------------------------------------------------------------------------
   * Save the record set as CSV in the path directory (not file).
   * --------------------------------------------------------------------------------
   */
  val OUTPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/CallInteratcion"
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