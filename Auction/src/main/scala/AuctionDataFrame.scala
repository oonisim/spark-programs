//--------------------------------------------------------------------------------
// MapR DEV 360 - Apache Spark Essentials
// https://www.mapr.com/services/mapr-academy/apache-spark-essentials
// Lab 2.2: Use DataFrames to load data into Spark
//--------------------------------------------------------------------------------
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math
import org.apache.spark.sql.SQLContext

case class Auctions(
  aucid: String,
  bid: Float,
  bidtime: Float,
  bidder: String,
  bidrate: Int,
  openbid: Float,
  price: Float,
  itemtype: String,
  dtl: Int)

class AuctionDataFrame {
  val AUCID = 0
  val BID = 1
  val BIDTIME = 2
  val BIDDER = 3
  val BIDRATE = 4
  val OPENBID = 5
  val PRICE = 6
  val ITEMTYPE = 7
  val DTL = 8

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AuctionDataFrame")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val inputRDD = sc.textFile("/user/wynadmin/auctiondata.csv").map(_.split(","))
    val auctionsRDD = inputRDD.map(a =>
      Auctions(
        a(AUCID),
        a(BID).toFloat,
        a(BIDTIME).toFloat,
        a(BIDDER),
        a(BIDRATE).toInt,
        a(OPENBID).toFloat,
        a(PRICE).toFloat,
        a(ITEMTYPE),
        a(DTL).toInt))
    val auctionsDF = auctionsRDD.toDF()

    //registering the DataFrame as a temporary table
    auctionsDF.registerTempTable("auctionsDF")

    // see the schema for the DataFrame
    auctionsDF.printSchema()

    // check the data in the DataFrame.
    val auctions = sqlContext.sql("SELECT * FROM auctionsDF limit 5")
    auctions.show()
    
    // total number of bids
    val totbids = auctionsDF.count()

    // number of distinct auctions
    val totitems = auctionsDF.select("aucid").distinct().count()
    println("number of distinct auctions is %d".format(totitems))

    // number of distinct itemtypes?
    val itemtypes = auctionsDF.select("itemtype").distinct().count()
    println("number of distinct item type is %d".format(itemtypes))

    // count of bids per auction and the item type (
    val counts = auctionsDF.groupBy("itemtype", "aucid").count()
    counts.show()
    val rows = sqlContext.sql("SELECT itemtype, aucid, count(*) FROM auctionsDF GROUP BY itemtype, aucid")
    rows.show()

  }
}