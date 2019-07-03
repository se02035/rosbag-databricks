# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC The ROS bag file format is a file format that is primarily optimized for write operations. As ROS bag files can become large (100s of GBs, or multiple TBs), there is a big need to process (read and analyse) this data efficiently (ideally using a distributed approach). One key step is to transform the ROS data from a row-oriented storage into a column-store (like Apache Parquet https://parquet.apache.org/). Having the data in Parquet enables us to efficiently process/query big data in a distributed manner using Apache Spark.
# MAGIC 
# MAGIC The organization *valtech* open-sourced an implementation of a splittable Hadoop InputFormat for the ROS bag files (https://github.com/valtech/ros_hadoop). This notebook uses valtech's implementation to efficiently load and process ROS bag files using Spark  

# COMMAND ----------

# MAGIC %md 
# MAGIC # 1. Prerequisites

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1.1 Libraries / Dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC Add the rosbag on hadoop libraries (can be found here https://github.com/valtech/ros_hadoop/releases) to the cluster (RosbagInput format)
# MAGIC - protobuf-java-3.3.0.jar
# MAGIC - rosbaginputformat.jar
# MAGIC - rosbaginputformat_2.11-0.9.8.jar
# MAGIC - scala-library-2.11.8.jar
# MAGIC 
# MAGIC The following Python libraries need to be added:
# MAGIC - opencv-python (https://pypi.org/project/opencv-python/)
# MAGIC - pycrypto (https://pypi.org/project/pycrypto/)
# MAGIC - rospy_message_converter (https://pypi.org/project/rospy_message_converter/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Creation of the ROS bag IDX file

# COMMAND ----------

# MAGIC %md
# MAGIC Extract the index configuration of your ROS bag file. The extracted index is a very very small configuration file containing a protobuf array that will be given in the job configuration. Note that the operation will not process and it will not parse the whole bag file, but will simply seek to the required offset. 
# MAGIC 
# MAGIC Currently, the recommendation is to extract the index file before uploading the entire ROS file to an Azure Storage account.
# MAGIC 
# MAGIC ```java
# MAGIC java -jar lib/rosbaginputformat.jar -f /opt/ros_hadoop/master/dist/HMB_4.bag
# MAGIC # will create an idx.bin config file /opt/ros_hadoop/master/dist/HMB_4.bag.idx.bin
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Spark configuration

# COMMAND ----------

# optional. use io cache to increase performance (in certain scenarios)
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Variables & constants

# COMMAND ----------

# input files - IDX file has to be created in advance
SOURCE_FILE_PATH_ROSBAG = "dbfs:/mnt/rosdata/dataset-2-2.bag"
SOURCE_FILE_PATH_ROSBAG_IDX = "/dbfs/mnt/rosdata/dataset-2-2.bag.idx.bin"

#output file
DESTINATION_PARQUET = "dbfs:/mnt/rosdata/parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Read the ROS bag file

# COMMAND ----------

# MAGIC %md
# MAGIC As defined in the ROS bag format specification, there are different types of ROS bag records. Here, we are only interested in records of type '*connection*' (op code '7') '*message*' (op code '2'). There is a 1:N relationship between Message and Connection records
# MAGIC 
# MAGIC - **Message** records contain the serialized message data in the ROS serialization format (includes the associated connection id)
# MAGIC 
# MAGIC - **Connection** records (represents a ROS topic) contain information of 
# MAGIC   1. ROS topics on which messages are stored 
# MAGIC   2. ROS data type of the messages that are associated with the connection (required for deserializing ROS messages)
# MAGIC 
# MAGIC This lead to the following process:
# MAGIC 1. Read the ROS bag file and filter for message with op code '7' (connection records) and op code '2' (message records)
# MAGIC 2. Extract information of the connection and message records
# MAGIC 3. Apply a basic, generic schema and create a Spark dataframe

# COMMAND ----------

fin = sc.newAPIHadoopFile(
    path =             SOURCE_FILE_PATH_ROSBAG,
    inputFormatClass = "de.valtech.foss.RosbagMapInputFormat",
    keyClass =         "org.apache.hadoop.io.LongWritable",
    valueClass =       "org.apache.hadoop.io.MapWritable",
    conf = {"RosbagInputFormat.chunkIdx":SOURCE_FILE_PATH_ROSBAG_IDX})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read all connection and message records

# COMMAND ----------

from pyspark.sql.types import LongType, IntegerType, StringType, StructType, StructField, BinaryType

field = [
  StructField("record_id",LongType(), True),
  StructField("op",IntegerType(), True),
  StructField("conn",IntegerType(), True),
  StructField("time",LongType(), True),
  StructField("topic",StringType(), True),
  StructField("dtype",StringType(), True),
  StructField("header", StringType(), True),
  StructField("data", StructType([
    StructField('message_definition', StringType(), True),
    StructField('md5sum', StringType(), True),
    StructField('msg_raw', BinaryType(), True),
  ]))
]

dfSchema = StructType(field)
sorted_fields = sorted(dfSchema.fields, key=lambda x: x.name)
sorted_schema = StructType(fields=sorted_fields)

df_records = sqlContext.createDataFrame(sc.emptyRDD(), sorted_schema)

# COMMAND ----------

from pyspark.sql import Row

def convert_to_row(rid, opid, connid, dheader, ddata):
  result_data = {}
  time = None
  topic = None
  dtype = None
  
  if opid == 7: # connection record
    topic = str(dheader.get('topic'))
    dtype = str(ddata.get('type'))
    ddata['message_definition'] = str(ddata.get('message_definition'))
    ddata['md5sum'] = str(ddata.get('md5sum'))
    result_data = ddata
  elif opid == 2: # message record
    time = dheader.get('time')
    result_data = {'msg_raw': ddata}
  
  return {'record_id':rid, 'op':opid, 'conn':connid, 'time':time, 'topic':topic, 'dtype':dtype, 'header':str(dheader), 'data':result_data}

# COMMAND ----------

from pyspark.sql import Row

df_records = fin \
  .filter(lambda r: r[1]['header'].get('op') ==  7 or 2) \
  .map(lambda r: Row(**convert_to_row(r[0], r[1]['header'].get('op'), r[1]['header'].get('conn'), r[1]['header'], r[1]['data']))) \
  .toDF(sorted_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Write to Parquet

# COMMAND ----------

df_records.write.mode("overwrite").parquet(DESTINATION_PARQUET)