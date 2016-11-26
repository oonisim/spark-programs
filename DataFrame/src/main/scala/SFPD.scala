import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

case class Input(
  event_timestamp: String,
  acct_id: String,
  call_id: String,
  numberDialed: String,
  menuId: String,
  menuDuration: String,
  menuInput: String,
  stype: String)

case class Incidents(incidentnum: String, category: String, description: String, dayofweek: String, date: String, time: String, pddistrict: String, resolution: String, address: String, X: Float, Y: Float, pdid: String)
object SFPD {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SFPD")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    /*
    val conf = new SparkConf().setAppName("IVR")
    val sc = new SparkContext(conf)
    */
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //val sfpdRDD = sc.textFile("file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/sfpd.csv").map(inc => inc.split(","))
    //val sfpdCase = sfpdRDD.map(inc => Incidents(inc(0), inc(1), inc(2), inc(3), inc(4), inc(5), inc(6), inc(7), inc(8), inc(9).toFloat, inc(10).toFloat, inc(11)))
    val sfpdRDD = sc.textFile("file:///D:/Home/Workspaces/Spark/DataFrame/src/main/resources/ivr_segments.csv").map(inc => inc.split(","))
    val sfpdCase = sfpdRDD.map(i => Input(i(0), i(1), i(2), i(3), i(4), i(5), i(6), i(7)))

    val sfpdDF = sfpdCase.toDF()

    sfpdDF.printSchema()

    sfpdDF.registerTempTable("sfpd")

    /*_________________________________________________________________________*/
    /*********************Lab 5.2 Explore data in DataFrames **************************/
    //1. Top 5 Districts
    val incByDist = sfpdDF.groupBy("pddistrict").count.sort($"count".desc).show(5)

    val topByDistSQL = sqlContext.sql("SELECT pddistrict, count(incidentnum) AS inccount FROM sfpd GROUP BY pddistrict ORDER BY inccount DESC LIMIT 5")
    //2. What are the top ten resolutions?
    val top10Res = sfpdDF.groupBy("resolution").count.sort($"count".desc)
    top10Res.show(10)
    val top10ResSQL = sqlContext.sql("SELECT resolution, count(incidentnum) AS inccount FROM sfpd GROUP BY resolution ORDER BY inccount DESC LIMIT 10")
    //3. Top 3 categories
    val top3Cat = sfpdDF.groupBy("category").count.sort($"count".desc).show(3)
    val top3CatSQL = sqlContext.sql("SELECT category, count(incidentnum) AS inccount FROM sfpd GROUP BY category ORDER BY inccount DESC LIMIT 3")
    //4. Save the top 10 resolutions to a JSON file.
    //top10ResSQL.toJSON.saveAsTextFile("/user/user01/output")
    /*_________________________________________________________________________*/
    /*********************Lab 5.3 User Defined Functions ***********************/

    //5.3.1 - UDF with SQL
    //1. define funciton 
    def getyear(s: String): String = {
      val year = s.substring(s.lastIndexOf('/') + 1)
      year
    }
    //2. register the function as a udf 
    sqlContext.udf.register("getyear", getyear _)

    //3. count inc by year
    val incyearSQL = sqlContext.sql("SELECT getyear(date), count(incidentnum) AS countbyyear FROM sfpd GROUP BY getyear(date) ORDER BY countbyyear DESC")
    incyearSQL.collect.foreach(println)

    //4. Category, resolution and address of reported incidents in 2014 
    val inc2014 = sqlContext.sql("SELECT category,address,resolution, date FROM sfpd WHERE getyear(date)='14'")
    inc2014.collect.foreach(println)

    //5. Vandalism only in 2014 with address, resolution and category
    val van2015 = sqlContext.sql("SELECT category,address,resolution, date FROM sfpd WHERE getyear(date)='15' AND category='VANDALISM'")
    van2015.collect.foreach(println)
    van2015.count

  }

}