import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object WordCount {
  val DELIM = "\\s"
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount").set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val lines = sc.textFile("hdfs:/user/spark/words.txt")
    println("Patitions = %d".format(lines.partitions.size))

    val strs = lines.flatMap(_.replaceAll("[,.!?:;]", "").toLowerCase.split("\\s"))
    val words = for(str <- strs if str.matches("[a-z]+")) yield (str, 1)
    val counts = words.reduceByKey(_ + _).sortBy(_._2, false)
    counts.collect.foreach(println)
  }
}

