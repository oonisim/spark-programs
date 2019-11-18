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

/**
 * Process to transform the data.
 */
object IVR {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IVR")
    val sc = new SparkContext(conf)
    //--------------------------------------------------------------------------------
    // 1. Remove records from the "IVR Segments" table. Transform, remove and add fields.
    //--------------------------------------------------------------------------------    
    IVRSegment.save(sc, IVRSegment.getRDD(sc))
    
    //--------------------------------------------------------------------------------    
    // 2. Create a new "IVR Menus" table where each record represents an IVR menu step.
    // (derived from the IVR Segments data). 
    //--------------------------------------------------------------------------------    
    IVRMenu.save(sc, IVRMenu.getRDD(sc))
    
    //--------------------------------------------------------------------------------
    // 3. Transform and add fields in the "Agent Call Segments" table.
    //--------------------------------------------------------------------------------    
    AgentCallSegment.save(sc, AgentCallSegment.getRDD(sc))

    //--------------------------------------------------------------------------------
    // 4. Create a new "Agent Call" table where each record represents an aggregated 
    // view of the agent call segments data
    //--------------------------------------------------------------------------------    
    AgentCall.save(sc, AgentCall.getDF(sc))

    //--------------------------------------------------------------------------------
    // Create a new "Call Interaction" table where each record represents an aggregated 
    // view of the ivr interaction and agent call.
    //--------------------------------------------------------------------------------    
    CallInteraction.save(sc, CallInteraction.getDF(sc))
    
  }
}