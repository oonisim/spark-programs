import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object NQueen {
  val n = 12
  def main(args: Array[String]): Unit = {
    // set up able configuration
    val sparkConf = new SparkConf()
      .setAppName("NQueen")
      .set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    //import sqlContext.implicits._
    //--------------------------------------------------------------------------------
    // [Logic]
    // Find if there are available columns for a queen to be placed at the current 'row'.
    // The columns where queens have been placed (placement(j) where j: (0 until row)) are taken.
    // The right diagonal columns for all placement(j), which is placement(j) + (row -j)), are taken.
    // The left  diagonal columns for all placement(j), which is placement(j) - (row -j)), are taken.
    // If there are available columns left which include the target 'column', then true.
    //--------------------------------------------------------------------------------
    def isSafe(column: Int, placement: List[Int]): Boolean = {
      val row = placement.length
      //val left = (0 until row).map(j => placement(j) + (row - j)).toSet
      //val left = (0 until row).map(j => placement(j) - (row - j)).toSet
      val right = for (j <- (0 until row) if (placement(j) + (row - j) < n)) yield placement(j) + (row - j)
      val left = for (j <- (0 until row) if (placement(j) - (row - j) >= 0)) yield placement(j) - (row - j)
      val all = (0 until n).toSet
      val taken = (placement.toSet ++ left.toSet ++ right.toSet)
      if ((all -- taken).contains(column)) true
      else false
    }

    //--------------------------------------------------------------------------------
    // Find out possible queen positions at the row.
    // [Data Structure]
    // placement : List[Int] is one possible placement of queens.
    // - List index is row number (0 to n -1)
    // - placement(row) is the position of a queen is placed at the row. 
    //
    // Example: At row 0, a queen is placed at column 5. At row 2, column 0.
    // Rows      :  0, 1, 2, 3, 4, 5, 6, 7
    // Placement : (5, 2, 0, 7, 3, 1, 6, 4)
    //--------------------------------------------------------------------------------
    def placeQueensAt(row: Int, placements: Set[List[Int]]): Set[List[Int]] = {
      if (row > n - 1) placements
      else {
        val next = for {
          placement <- placements
          col <- 0 until n
          if (isSafe(col, placement))
        } yield {
          placement ::: List(col)
        }
        println("Possible positions up to row %d is %s".format(row, next))
        placeQueensAt(row + 1, next)
      }
    }
    def queensAtFirst: List[List[Int]] = {
      val placements = for {
        col <- 0 until n
      } yield {
        List(col)
      }
      placements.toList
    }

    val initial = sc.parallelize(queensAtFirst, 12)
    //val initial = sc.parallelize(queensAtFirst)
    println("----------> Numberf of partitions = %d".format(initial.partitions.size))
    
    val result = initial.flatMap(x => placeQueensAt(1, Set(x))).collect()
    println("----------> Result size %d".format(result.size))
    result.take(5).foreach { x => println(x) }
    //result.saveAsTextFile("hdfs:/user/wynadmin/nqueen")
  }
}
