name := "Auction Project"
 
version := "1.0"
 
//scalaVersion := "2.11.8"
scalaVersion := "2.10.6"

/* 
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.2",
    "org.apache.spark" %% "spark-sql" % "1.6.2",
    "org.apache.spark" %% "spark-mllib" % "1.6.2"
)
*/

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.6.2" % "provided",
    "org.apache.spark" %% "spark-mllib" % "1.6.2" % "provided"
)
