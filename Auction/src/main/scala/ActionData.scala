//--------------------------------------------------------------------------------
// MapR DEV 360 - Apache Spark Essentials
// https://www.mapr.com/services/mapr-academy/apache-spark-essentials
// Lesson 2 - Load and Inspect Data
//--------------------------------------------------------------------------------
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math

object AuctionData {
  val AUCID = 0
  val BID = 1
  val BIDTIME = 2
  val BIDDER = 3
  val BIDRATE = 4
  val OPENBID = 5
  val PRICE = 6
  val ITEMTYPE = 7
  val DTL = 8

  def section = println("--------------------------------------------------------------------------------")
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AuctionData")
    val sc = new SparkContext(conf)

    val auctionRDD = sc.textFile("hdfs:/user/wynadmin/auctiondata.csv").map(_.split(","))
    auctionRDD.cache()

    // First auction data.
    println("First element of auction is %s".format(auctionRDD.first()))

    // First 5 auction data.
    println("First 5 element of auction is %s".format(auctionRDD.take(5)))

    // Total number of bids
    val totbids = auctionRDD.count()
    section
    println("Total number of bids = %d".format(totbids))

    // Total number of distict bids.
    val totitems = auctionRDD.map(bid => bid(AUCID)).distinct().count()
    section
    println("Total number of distict bids = %d".format(totitems))

    // Total number of item types that were auctioned?
    val totitemtype = auctionRDD.map(bid => bid(ITEMTYPE)).distinct().count()
    section
    println("Total number of distict item types = %d".format(totitemtype))

    // Total number of bids per item type?
    val bids_itemtype = auctionRDD.map(bid => (bid(ITEMTYPE), 1)).reduceByKey(_ + _)
    section
    println("Total number of bids per item type = %s".format(bids_itemtype.take(5)))

    //--------------------------------------------------------------------------------
    // We want to calculate the max, min and average number of bids among all the auctioned items.
    //--------------------------------------------------------------------------------
    // Create an RDD that contains total bids for each auction.
    val bidsAuctionRDD = auctionRDD.map(bid => (bid(AUCID), 1)).reduceByKey(_ + _)
    val bidsItemRDD = bidsAuctionRDD.map(_._2)
    bidsItemRDD.cache()
    
    section
    println("Bids per auction for the first 5 auciton items = %s".format(bidsItemRDD.take(5)))

    // Across all auctioned items, what is the maximum number of bids? 
    val maxbids = bidsItemRDD.reduce(Math.max)
    section
    println("maximum number of bids is %d".format(maxbids))

    // Across all auctioned items, what is the minimum number of bids?
    val minbids = bidsItemRDD.reduce(Math.min)
    section
    println("minimum number of bids is %d".format(minbids))

    // What is the average number of bids?
    val avgbids = bidsItemRDD.reduce(_ + _) / bidsItemRDD.count()
    section
    println("average number of bids is %d".format(avgbids))
  }
}