import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

object Social {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Flights").set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val folder = "/user/wynadmin/"

    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50)))
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3))

    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    // users that are at least 30 years old
    val adults = for ((id, (name, age)) <- graph.vertices if (age >= 30)) yield "%s is %d".format(name, age)
    adults.collect().foreach(println)

    // who likes who
    val favors = for {
      triplet <- graph.triplets
      (srcid, (srcname, srcage)) <- vertexArray if (srcid == triplet.srcId)
      (dstid, (dstname, dstage)) <- vertexArray if (dstid == triplet.dstId)
    } yield ("%s likes %s".format(srcname, dstname))
    favors.collect.foreach(println)

    // mutual
    val mutuals = for {
      triplet <- graph.triplets if (triplet.attr > 5)
      (srcid, (srcname, srcage)) <- vertexArray if (srcid == triplet.srcId)
      (dstid, (dstname, dstage)) <- vertexArray if (dstid == triplet.dstId)
    } yield ("%s and %s like each other".format(srcname, dstname))
    mutuals.collect.foreach(println)

    // Count of likes
    for {
      (vid, count) <- graph.inDegrees
      (uid, (name, age)) <- vertexArray if (uid == vid)
    } println("%s is liked by %d people".format(name, count))
  }

}