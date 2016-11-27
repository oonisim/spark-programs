[Results]
Generated CSV files are in results directory.

[Approach]
Used Scala 2.11.8 and Apache Spark 2.0.2 Spark SQL and DataFrame/RDD.

[IDE]
Scala source files are in src/main/scala with build.sbt. 
Run sbt eclipse in the IVR directory to crate an Eclipse project.
Remove header lines from the input files (cat file | tail -n +2).

[Execution]
%SPARK_HOME%\bin\spark-submit.cmd --class ETL --master local[1] dataframe_2.11-1.0.jar > log.txt

[ZIP structure]
IVR
├── README.txt
├── results
│   ├── AgentCall.csv
│   ├── AgentCallSegment.csv
│   ├── CallInteratcion.csv
│   ├── IVRMenu.csv
│   └── IVRSegment.csv
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