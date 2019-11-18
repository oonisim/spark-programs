//import sbt.Resolver

lazy val commonSettings = Seq(
  organization := "com.example",
  version := "1.0",
  scalaVersion := "2.11.12"
)
val sparkVersion = "2.4.4"

lazy val root = (project in file(".")).
  settings(
    commonSettings,
    name := "IVR",
    mainClass in Compile := Some("IVR")
  )

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

// https://stackoverflow.com/questions/51156969/what-is-an-sbt-resolver
resolvers += Resolver.sonatypeRepo("releases")
