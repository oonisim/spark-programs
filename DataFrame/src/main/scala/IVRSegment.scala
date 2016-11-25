import java.sql.Timestamp

case class IVRSegment(
  start_time: String,
  end_time: String,
  duration: Int,
  acct_id: String,
  call_id: String,
  numberDialed: String,
  menu_count: Int)

object IVRSegment {
  val START_TIME = 0
  val END_TIME = 1
  val DURATION = 2
  val ACCT_ID = 3
  val CALL_ID = 4
  val NUMBER_DIALED = 5
  val MENU_COUNT = 6
}