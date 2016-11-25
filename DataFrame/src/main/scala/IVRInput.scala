import java.sql.Timestamp

case class IVRInput(
  event_timestamp: String,
  acct_id: String,
  call_id: String,
  numberDialed: String,
  menuId: String,
  menuDuration: String,
  menuInput: String,
  stype: String)

object IVRInput {
  val TIMESTAMP = 0
  val ACCT_ID = 1
  val CALL_ID = 2
  val NUMBER_DIALED = 3
  val MENU_ID = 4
  val MENU_DURATION = 5
  val MENU_INPUT = 6
  val STYPE = 7
}