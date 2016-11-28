[Approach]
Used Scala 2.11.8 and Apache Spark 2.0.2 Spark SQL and DataFrame/RDD.

[IDE]
Scala source files are in src/main/scala with build.sbt. 
Run sbt eclipse in the IVR directory to crate an Eclipse project.
Remove header lines from the input files (cat file | tail -n +2).

[Execution]
%SPARK_HOME%\bin\spark-submit.cmd 
  --conf spark.sql.warehouse.dir=file:///D:/Home/Workspaces/Spark/IRV 
  --class ETL 
  --master local[4] 
  target\scala-2.11\ivr_2.11-1.0.jar
  
[ZIP structure]
IVR
├── README.txt
├── build.sbt
└── src
    └── main
        ├── resources
        │   ├── agent_call_segments.csv <--- Header line removed
        │   └── ivr_segments.csv        <--- Header line removed
        └── scala
            ├── AgentCall.scala
            ├── AgentCallInput.scala
            ├── AgentCallSegment.scala
            ├── CallInteraction.scala
            ├── IVR.scala
            ├── IVRInput.scala
            ├── IVRMenu.scala
            ├── IVRSegment.scala
            └── Utility.scala