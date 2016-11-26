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

case class AgentCall(
  event_timestamp: Timestamp,
  answer_time_timestamp: Timestamp,
  hangup_time_timestamp: Timestamp,
  end_time_timestamp: Timestamp,
  acct_id: String,
  call_id: String,
  sequence_no: String,
  trans_to: String,
  prim_agent: String,
  agentID: String,
  agentSkillId: String,
  agentGroupId: String,
  callType: String,
  callDisposition: String,
  networkTime: Int,
  duration: Int,
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

object AgentCall {
  val EVENT_TIMESTAMP_COLUMN = 0
  val ANSWER_TIME_TIMESTAMP_COLUMN = 1
  val HANGUP_TIME_TIMESTAMP_COLUMN = 2
  val END_TIME_TIMESTAMP_COLUMN = 3
  val ACCT_ID_COLUMN = 4
  val CALL_ID_COLUMN = 5
  val SEQUENCE_NO_COLUMN = 6
  val TRANS_TO_COLUMN = 7
  val PRIM_AGENT_COLUMN = 8
  val AGENTID_COLUMN = 9
  val AGENTSKILLID_COLUMN = 10
  val AGENTGROUPID_COLUMN = 11
  val CALLTYPE_COLUMN = 12
  val CALLDISPOSITION_COLUMN = 13
  val NETWORKTIME_COLUMN = 14
  val DURATION_COLUMN = 15
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

  val INPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/agent_call_segments.csv"
  val OUTPUT_FILE = "file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/calls"
  val CSV_SEPARATOR = ","
  val MULTIFIELD_SEPARATOR = "~"
  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSS"

  def getRDD(sc: SparkContext): RDD[AgentCall] = {
    val records = sc.textFile(INPUT_FILE).map(line => line.split(CSV_SEPARATOR))
    val calls = for {
      fields <- records
    } yield {
      AgentCall(
        getTimestamp(fields(EVENT_TIMESTAMP_COLUMN), TIMESTAMP_FORMAT).get,
        getTimestamp(fields(ANSWER_TIME_TIMESTAMP_COLUMN), TIMESTAMP_FORMAT).get,
        getTimestamp(fields(HANGUP_TIME_TIMESTAMP_COLUMN), TIMESTAMP_FORMAT).get,
        getTimestamp(fields(END_TIME_TIMESTAMP_COLUMN), TIMESTAMP_FORMAT).get,
        fields(ACCT_ID_COLUMN),
        fields(CALL_ID_COLUMN),
        fields(SEQUENCE_NO_COLUMN),
        fields(TRANS_TO_COLUMN),
        fields(PRIM_AGENT_COLUMN),
        fields(AGENTID_COLUMN),
        fields(AGENTSKILLID_COLUMN),
        fields(AGENTGROUPID_COLUMN),
        fields(CALLTYPE_COLUMN),
        fields(CALLDISPOSITION_COLUMN),
        fields(NETWORKTIME_COLUMN).toInt,
        fields(DURATION_COLUMN).toInt,
        fields(RINGTIME_COLUMN).toInt,
        fields(DELAYTIME_COLUMN).toInt,
        fields(TIMETOABAND_COLUMN).toInt,
        fields(HOLDTIME_COLUMN).toInt,
        fields(TALKTIME_COLUMN).toInt,
        fields(WORKTIME_COLUMN).toInt,
        fields(LOCALQTIME_COLUMN).toInt,
        fields(CONFERENCETIME_COLUMN).toInt,
        fields(CALLREASON_COLUMN),
        fields(PRODUCTTYPE_COLUMN))
    }
    calls
  }
  def getDF(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    getRDD(sc).toDF()
  }
  def save(sc: SparkContext, rdd: RDD[AgentCall], path: String = OUTPUT_FILE): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    rdd.toDF().coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
}