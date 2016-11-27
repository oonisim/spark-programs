lazy val root = (project in file(".")).
  settings(
    name := "IVR",
    version := "1.0",
	scalaVersion := "2.11.8",
    mainClass in Compile := Some("IVR")        
  )

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.0.2" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.0.2" % "provided"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
