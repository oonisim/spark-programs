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
 * Record set of the Agent call segments.
 * Mostly pass through from the input file except duration.
 *
 * [Fields]
 * duration	: calculated field: end_time - start_time (in seconds)
 * --------------------------------------------------------------------------------
 */
case class AgentCallSegment(
  start_time: Timestamp,
  end_time: Timestamp,
  duration: Int,
  acct_id: String,
  call_id: String,
  answer_time_timestamp: Timestamp,
  hangup_time_timestamp: Timestamp,
  sequence_no: String,
  trans_to: String,
  prim_agent: String,
  agentID: String,
  agentSkillId: String,
  agentGroupId: String,
  callType: String,
  callDisposition: String,
  networkTime: Int,
  ringTime: Int,
  delayTime: Int,
  timeToAband: Int,
  holdTime: Int,
  talkTime: Int,
  workTime: Int,
  localQTime: Int,
  conferenceTime: Int,
  callReason: String,
  productType: String)

object AgentCallSegment {
  val START_TIME_COLUMN = 0
  val END_TIME_COLUMN = 1
  val DURATION_COLUMN = 2
  val ACCT_ID_COLUMN = 3
  val CALL_ID_COLUMN = 4
  val ANSWER_TIME_TIMESTAMP_COLUMN = 5
  val HANGUP_TIME_TIMESTAMP_COLUMN = 6
  val SEQUENCE_NO_COLUMN = 7
  val TRANS_TO_COLUMN = 8
  val PRIM_AGENT_COLUMN = 9
  val AGENTID_COLUMN = 10
  val AGENTSKILLID_COLUMN = 11
  val AGENTGROUPID_COLUMN = 12
  val CALLTYPE_COLUMN = 13
  val CALLDISPOSITION_COLUMN = 14
  val NETWORKTIME_COLUMN = 15
  val RINGTIME_COLUMN = 16
  val DELAYTIME_COLUMN = 17
  val TIMETOABAND_COLUMN = 18
  val HOLDTIME_COLUMN = 19
  val TALKTIME_COLUMN = 20
  val WORKTIME_COLUMN = 21
  val LOCALQTIME_COLUMN = 22
  val CONFERENCETIME_COLUMN = 23
  val CALLREASON_COLUMN = 24
  val PRODUCTTYPE_COLUMN = 25

  /**
   * --------------------------------------------------------------------------------
   * Record set in RDD.
   * --------------------------------------------------------------------------------
   */
  def getRDD(sc: SparkContext): RDD[AgentCallSegment] = {
    val inputs = AgentCallInput.getRDD(sc)

    val segments = for {
      input <- inputs
    } yield {
      val duration = ((input.end_time_timestamp.getTime - input.event_timestamp.getTime) / 1000).toInt
      AgentCallSegment(
        input.event_timestamp,
        input.end_time_timestamp,
        duration,
        input.acct_id,
        input.call_id,
        input.answer_time_timestamp,
        input.hangup_time_timestamp,
        input.sequence_no,
        input.trans_to,
        input.prim_agent,
        input.agentID,
        input.agentSkillId,
        input.agentGroupId,
        input.callType,
        input.callDisposition,
        input.networkTime,
        input.ringTime,
        input.delayTime,
        input.timeToAband,
        input.holdTime,
        input.talkTime,
        input.workTime,
        input.localQTime,
        input.conferenceTime,
        input.callReason,
        input.productType)
    }
    segments
  }
  /**
   * --------------------------------------------------------------------------------
   * Record set in DataFrame
   * --------------------------------------------------------------------------------
   */
  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    getRDD(sc).toDF()
  }
  /**
   * --------------------------------------------------------------------------------
   * Save the record set as CSV in the path directory (not file).
   * --------------------------------------------------------------------------------
   */
  val OUTPUT_FILE = "src/main/resources/AgentCallSegment"
  def save(sc: SparkContext, rdd: RDD[AgentCallSegment], path: String = OUTPUT_FILE): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF().coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv(path)
  }
}