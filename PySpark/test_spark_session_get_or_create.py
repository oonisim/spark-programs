import os
import sys

# --------------------------------------------------------------------------------
# Constant
# --------------------------------------------------------------------------------
SPARK_HOME = "/opt/homebrew/Cellar/apache-spark/3.3.1/libexec"
JAVA_HOME = '/opt/homebrew/opt/openjdk'

# --------------------------------------------------------------------------------
# Environment Variables
# NOTE:
# SPARK_HOME must be set to /opt/homebrew/Cellar/apache-spark/3.3.1/libexec",
# NOT /opt/homebrew/Cellar/apache-spark/3.3.1".
# Otherwise Java gateway process exited before sending its port number in java_gateway.py
# --------------------------------------------------------------------------------
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ['JAVA_HOME'] = JAVA_HOME
sys.path.extend([
    f"{SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip",
    f"{SPARK_HOME}/python/lib/pyspark.zip",
])


from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()
print("spark session created")
del spark
