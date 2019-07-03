# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction

# COMMAND ----------

# MAGIC %md 
# MAGIC # 1. Configuration & Initialization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark configuration

# COMMAND ----------

# optional. use io cache to increase performance (in certain scenarios)
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables & constants

# COMMAND ----------

# input files - IDX file has to be created in advance
SOURCE_PARQUET = "dbfs:/mnt/rosdata/parquet"

#output file
DESTINATION_PARQUET = "dbfs:/mnt/rosdata/parquet_prepared"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize

# COMMAND ----------

def update_ros_environment():
  import os
  import sys
  
  missing_ros_module_path_0 = '/opt/ros/kinetic/lib/python2.7/dist-packages'
  missing_ros_module_path_1 = '/usr/lib/python2.7/dist-packages'

  # PYTHONPATH will not be correctly set on the driver. 
  # The PYTHONPATH needs to be updated if you need to run a bash command that
  # references the ROS python modules (includes the ROS tools like rosbag, rostopic, etc)
  if missing_ros_module_path_0 not in os.environ['PYTHONPATH']:
    os.environ['PYTHONPATH'] = os.environ['PYTHONPATH'] + ':' + missing_ros_module_path_0  + ':' + missing_ros_module_path_1

  # Update sys.path to be able to use the ROS python modules in the Databricks notebook (pyspark)
  if missing_ros_module_path_0 not in sys.path:
    sys.path.insert(0,missing_ros_module_path_0)
    sys.path.insert(0,missing_ros_module_path_1)
    
    
update_ros_environment()

# COMMAND ----------

df_records = spark.read.parquet(SOURCE_PARQUET)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Get ROS message data

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.1 Join ROS messages with connections  

# COMMAND ----------

# MAGIC %md
# MAGIC Since the '*connection* records contain the message definition (which is required to deserialize ROS messages), we have to join the ROS message with the correct connection records. 

# COMMAND ----------

from pyspark.sql.functions import broadcast
 
df_connections = df_records.where('op == 7') \
  .select('conn', 'dtype', 'topic', 'data.message_definition', 'data.md5sum') \
  .dropDuplicates()
df_connections.cache()

df_messages = df_records.where('op == 2') \
  .select('record_id', 'time', 'conn','header', 'data') \
  .join(broadcast(df_connections), on=['conn'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Deserialize ROS message payload

# COMMAND ----------

# MAGIC %md
# MAGIC Use the ROS libraries to deserialize the message content. In order to simply the processing in the subsequent process, the message payload will be converted to JSON. As the final step, the messages' timestamp information will be extracted and stored as a Spark timestamp. 

# COMMAND ----------

from pyspark.sql.functions import udf

def msg_map_main_ros_msg(data_type, md5sum, msg_def, data):

  # update the references and ensure that the ROS libs will be found
  update_ros_environment()
  
  from collections import namedtuple
  from rosbag.bag import _get_message_type
  
  c = {'md5sum':md5sum, 'datatype':data_type, 'msg_def':msg_def }
  c = namedtuple('GenericDict', c.keys())(**c)
  
  msg_type = _get_message_type(c)
  msg = msg_type() 
  res = msg.deserialize(data)
  
  return res

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructField, StructType

field = [
  StructField("seq", LongType(), True), # part of header
  StructField("secs", LongType(), True),
  StructField("nsecs", LongType(), True),
  StructField("frame_id", StringType(), True), 
  StructField("msg", StringType(), True)
]

generic_message_schema = StructType(field)
 

def to_generic_message(dtype, md5sum, msg_def, data):
  
  # update the references and ensure that the ROS libs will be found
  update_ros_environment()
  
  from collections import namedtuple  
  import json
  from rospy_message_converter import message_converter
  
  # Step #1: deserialize the rosbag message record type
  ros_msg = msg_map_main_ros_msg(dtype, md5sum, msg_def, data['msg_raw'])
  
  # Step #2: deserialize the specific message payload & create schematized row entry#
  h = {'seq' : None, 'secs': 0, 'nsecs':0, 'frame_id':None}  
  if hasattr(ros_msg, 'header'):
    h['seq'] = ros_msg.header.seq
    h['secs'] = ros_msg.header.stamp.secs
    h['nsecs'] = ros_msg.header.stamp.nsecs
    h['frame_id'] = str(ros_msg.header.frame_id)
    
  h = namedtuple('GenericDict', h.keys())(**h)
  
  # Step #3: convert the ros message into a json message for later processing
  dictionary = message_converter.convert_ros_message_to_dictionary(ros_msg)
  
  # convert every item to string (to avoid the bytearray issue)
  dictionary = {k: str(v) for k,v in dictionary.iteritems()}
  
  json_message = json.dumps(dictionary)
  
  return (h.seq,h.secs,h.nsecs,h.frame_id,json_message)

to_generic_message_udf = udf(to_generic_message, generic_message_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Extract timestamp of ROS message

# COMMAND ----------

from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf
import datetime

def extract_timestamp(secs, nsecs):
  s = secs + (float(nsecs) / 1000000000)
  return datetime.datetime(1970, 1,1) + datetime.timedelta(seconds=s)
  
extract_timestamp_udf = udf(extract_timestamp, TimestampType())

# COMMAND ----------

from pyspark.sql.functions import col

df_messages = df_messages \
  .withColumn("payload", to_generic_message_udf(col('dtype'), col('md5sum'), col('message_definition'), col('data'))) \
  .withColumn("tstamp", extract_timestamp_udf(col('payload.secs'), col('payload.nsecs'))) \
  .select('tstamp', 'record_id', 'dtype', 'topic', 'payload')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Write to Parquet

# COMMAND ----------

# save as parquet (here: folder mounted to ADLS Gen2)
df_messages.write.mode("overwrite").parquet(DESTINATION_PARQUET)