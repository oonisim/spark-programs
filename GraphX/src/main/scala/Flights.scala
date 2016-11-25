import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

object Flights {
  case class Flight(
    dofM: String,
    dofW: String,
    carrier: String,
    tailnum: String,
    flnum: Int,
    org_id: Long,
    origin: String,
    dest_id: Long,
    dest: String,
    crsdeptime: Double,
    deptime: Double,
    depdelaymins: Double,
    crsarrtime: Double,
    arrtime: Double,
    arrdelay: Double,
    crselapsedtime: Double,
    dist: Int)

  def parseFlight(str: String): Flight = {
    val line = str.split(",")
    Flight(
      line(0),
      line(1),
      line(2),
      line(3),
      line(4).toInt,
      line(5).toLong,
      line(6),
      line(7).toLong,
      line(8),
      line(9).toDouble,
      line(10).toDouble,
      line(11).toDouble,
      line(12).toDouble,
      line(13).toDouble,
      line(14).toDouble,
      line(15).toDouble,
      line(16).toInt)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Flights").set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val folder = "/user/wynadmin/"

    //--------------------------------------------------------------------------------
    // January 2014 flight data 
    //--------------------------------------------------------------------------------    
    val textRDD = sc.textFile(folder + "/" + "rita2014jan.csv")
    val flightsRDD = textRDD.map(parseFlight).cache()

    //--------------------------------------------------------------------------------        
    // Airport as vertex
    // AirportID is numerical - Mapping airport ID to the 3-letter code
    //--------------------------------------------------------------------------------        
    val airports = flightsRDD.map(flight => (flight.org_id, flight.origin)).distinct
    val airportMap = airports.map { case ((org_id), name) => (org_id -> name) }.collect.toList.toMap
    // Default vertex
    val nowhere = "nowhere"

    //--------------------------------------------------------------------------------        
    // Defining the routes as edges
    //--------------------------------------------------------------------------------            
    val routes = flightsRDD.map(flight => ((flight.org_id, flight.dest_id), flight.dist)).distinct
    routes.cache
    val edges = routes.map { case ((org_id, dest_id), distance) => Edge(org_id.toLong, dest_id.toLong, distance) }

    //--------------------------------------------------------------------------------        
    // Graph as airport-route-airport
    //--------------------------------------------------------------------------------            
    val graph = Graph(airports, edges, nowhere)

    // Number of routes
    val numroutes = graph.numEdges
        
    //Sort and print out the longest distance routes
    graph.triplets.sortBy(_.attr, false, 3).take(10).foreach(triplet => println("Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + "."))
    
    
  }
}
