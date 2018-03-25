
IVR (Interactive Voice Recording) ETL using Apache Spark
=========

Task
------------

A client has provided you with two data extracts that contain information around customer calls to a contact centre. Before the data can be loaded and analyzed in a customer journey model it needs to be transformed. You have been asked to develop a transformation routine that takes the two data extracts as an input and creates a set of five output files that can be loaded directly into a customer journey model. It is anticpiated that the client will provide updated data extracts on an ongoing basis.

<img src="https://github.com/oonisim/Apache-Spark/blob/master/IVR/images/ER.png" width=400>

The first table [ivr_segments.csv](https://github.com/oonisim/Apache-Spark/tree/master/IVR/data/ivr_segments.csv) represents IVR Segments. Each record includes a set of attributes that describe the customer's traversal through the IVR. The second table [agent_call_segments.csv](https://github.com/oonisim/Apache-Spark/tree/master/IVR/data/agent_call_segments.csv) represents Agent Call Segments. A customer might talk to multiple agents during a call (i.e., the call is transferred from agent to agent). Each record in this table represents one segment of a call. A detailed description for each field is provieded Field Description tab in the [specifications](https://github.com/oonisim/Apache-Spark/tree/master/IVR/data/specifications.xlsx

You have been asked develop a process to transform the data in the following way:

1. Remove records from the "IVR Segments" table. Transform, remove and add fields.
2. Create a new "IVR Menus" table where each record represents an IVR menu step (derived from the IVR Segments data).
3. Transform and add fields in the "Agent Call Segments" table.
4. Create a new "Agent Call" table where each record represents an aggregated view of the agent call segments data
5. Create a new "Call Interaction" table where each record represents an aggregated view of the ivr interaction and agent call.

The exact requirements for each table can be found on the "Output Requirements" tab in the specification.

---

Approach
------------
Used Scala 2.11.8 and Apache Spark 2.0.2 Spark SQL and DataFrame/RDD.

#### Eclipse setup
Build is with build.sbt. Scala source files are in src/main/scala . Run sbt eclipse in the IVR directory to crate an Eclipse project. Remove header lines from the input files (cat file | tail -n +2).

[Execution]
%SPARK_HOME%\bin\spark-submit.cmd
  --conf spark.sql.warehouse.dir=file:///D:/Home/Workspaces/Spark/IRV
  --class ETL
  --master local[4]
  target\scala-2.11\ivr_2.11-1.0.jar

#### Repository tructure
```
IVR
├── README.md
├── data      <--- Original input data and the specification.
├── results   <--- Output data
├── build.sbt
└── src
    └── main
        ├── resources                   <--- Input data
        │   ├── agent_call_segments.csv <--- Header line removed
        │   └── ivr_segments.csv        <--- Header line removed
        └── scala
            ├── IVR.scala               <--- Main
            ├── IVRSegment.scala        <--- Step 1: Remove records from the "IVR Segments" table. Transform, remove and add fields.
            ├── IVRInput.scala          <---         For step 1, load ivr_segment.csv to create a record set of the original IVR segment including DUP.
            ├── IVRMenu.scala           <--- Step 2: Create a new "IVR Menus" table where each record represents an IVR menu step (derived from the IVR Segments data).
            ├── AgentCallSegment.scala  <--- Step 3: Calculate duration field by end_time - start_time (in seconds).
            ├── AgentCall.scala         <--- Step 4: Create one record for each call_id that exists in agent call segments table.
            ├── AgentCallInput.scala    <---         For step 4, load agent_call_segments.csv to create a record set of the original Agent call segments.
            ├── CallInteraction.scala   <--- Step 5: Create one record for each call_id that exists in ivr_segment and/or agent_call table.
            └── Utility.scala
```