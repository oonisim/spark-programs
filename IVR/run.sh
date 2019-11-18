#!/bin/bash
$SPARK_HOME/bin/spark-submit \
  --class IVR \
  --master local[4] \
  target/scala-2.11/IVR-assembly-1.0.jar