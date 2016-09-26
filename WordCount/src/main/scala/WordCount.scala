import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object WordCount {
  def main(args: Array[String]): Unit = {
    // set up able configuration
    val sparkConf = new SparkConf()
      .setAppName("WordCount")
      .set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._
    val lines = sc.textFile("hdfs:/user/wynadmin/sfpd.csv")
    val words = for (line <- lines; word <- line.split(",") if word.toLowerCase.matches("[a-z]+")) yield (word, 1)
    val counts = words.reduceByKey(_ + _)
    counts.take(10).foreach(println)
  }
}
