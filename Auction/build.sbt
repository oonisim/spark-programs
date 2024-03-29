lazy val root = (project in file(".")).
  settings(
    name := "Auction Project",
    version := "1.0",
	scalaVersion := "2.11.8",
	//scalaVersion := "2.10.6",
    mainClass in Compile := Some("ActionDataFrame")        
  )
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

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
