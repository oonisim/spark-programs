lazy val root = (project in file(".")).
  settings(
    name := "WordCount",
    version := "1.0",
	scalaVersion := "2.11.8",
    mainClass in Compile := Some("WordCount")        
  )
/*  
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0",
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "org.apache.spark" %% "spark-mllib" % "2.2.0"
)
*/

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
