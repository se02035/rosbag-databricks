# Databricks notebook source
# MAGIC %md 
# MAGIC # Configuration & Initialization

# COMMAND ----------

# MAGIC %md
# MAGIC Use a Databricks ML cluster (e.g. 5.4)
# MAGIC 
# MAGIC Add the rosbag on hadoop libraries (can be found here https://github.com/valtech/ros_hadoop/releases) to the cluster (RosbagInput format)
# MAGIC - protobuf-java-3.3.0.jar
# MAGIC - rosbaginputformat.jar
# MAGIC - rosbaginputformat_2.11-0.9.8.jar
# MAGIC - scala-library-2.11.8.jar
# MAGIC 
# MAGIC The following Python libraries need to be added:
# MAGIC - opencv-python
# MAGIC - pycrypto

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark configuration

# COMMAND ----------

# OPTIONAL
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables & constants

# COMMAND ----------

# Azure storage endpoint
ADLS_NAME = "oliadls"
ADLS_FS = "data"
ADLS_MOUNT_POINT = "/mnt/rosdata"

# input files - IDX file has to be created in advance
FILE_PATH_ROSBAG = "dbfs:/mnt/rosdata/dataset-2-2.bag"
FILE_PATH_ROSBAG_IDX = "/dbfs/mnt/rosdata/dataset-2-2.bag.idx.bin"

#output file
PARQUET_DEST = "dbfs:/mnt/rosdata/parquet"

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

# MAGIC %md 
# MAGIC ### Mount an ADLS gen 2 instance

# COMMAND ----------

application_id = "xxxx" 
client_secret = "xxx"
tenant = "xxx"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": application_id,
       "fs.azure.account.oauth2.client.secret": client_secret,
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + tenant + "/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://" + ADLS_FS + "@" + ADLS_NAME + ".dfs.core.windows.net/",
mount_point = ADLS_MOUNT_POINT,
extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the rosbag file

# COMMAND ----------

fin = sc.newAPIHadoopFile(
    path =             FILE_PATH_ROSBAG,
    inputFormatClass = "de.valtech.foss.RosbagMapInputFormat",
    keyClass =         "org.apache.hadoop.io.LongWritable",
    valueClass =       "org.apache.hadoop.io.MapWritable",
    conf = {"RosbagInputFormat.chunkIdx":FILE_PATH_ROSBAG_IDX})

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

# save as parquet (here: folder mounted to ADLS Gen2)
df_records.write.mode("overwrite").parquet(PARQUET_DEST)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the parquet data

# COMMAND ----------

parquetDF = spark.read.parquet(PARQUET_DEST)

# COMMAND ----------

from pyspark.sql.functions import broadcast
 
df_connections = parquetDF.where('op == 7') \
  .select('conn', 'dtype', 'topic', 'data.message_definition', 'data.md5sum') \
  .dropDuplicates()
df_connections.cache()

df_messages = parquetDF.where('op == 2') \
  .select('record_id', 'time', 'conn','header', 'data') \
  .join(broadcast(df_connections), on=['conn'])

# COMMAND ----------

from pyspark.sql.functions import udf

def msg_map_main_ros_msg(data_type, md5sum, msg_def, data):

  # hack initialize on worker
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

# MAGIC %md
# MAGIC ### dataspeed_can_msgs_CanMessageStamped

# COMMAND ----------

df_filtered_msgs = df_messages.where("topic == '/can_bus_dbw/can_rx'")

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, BinaryType, BooleanType, StructField, IntegerType, StructType
from pyspark.sql.functions import udf

field = [
  StructField("record_id",LongType(), True),
  StructField("time",LongType(), True),
  StructField("topic",StringType(), True),
  StructField("dtype",StringType(), True),
  StructField("seq", LongType(), True), # part of header
  StructField("secs", LongType(), True),
  StructField("nsecs", LongType(), True),
  StructField("frame_id", StringType(), True), 
  StructField("data", BinaryType(), True), # part of the message 
  StructField("id", IntegerType(), True),
  StructField("extended", BooleanType(), True),
  StructField("dlc", IntegerType(), True)
]

dataspeed_can_msgs_CanMessageStamped_schema = StructType(field)
sorted_fields = sorted(dataspeed_can_msgs_CanMessageStamped_schema.fields, key=lambda x: x.name)
dataspeed_can_msgs_CanMessageStamped_schema_sorted_schema = StructType(fields=sorted_fields)

def to_dataspeed_can_msgs_CanMessageStamped(r):

  # Step #1: deserialize the rosbag message record type
  ros_msg = msg_map_main_ros_msg(r['dtype'], r['md5sum'], r['message_definition'], r['data']['msg_raw'])
  
  # Step #2: deserialize the specific message payload & create schematized row entry
  return {'record_id':r['record_id'], 
          'time':r['time'], 
          'topic':r['topic'], 
          'dtype':r['dtype'], 
          'seq':ros_msg.header.seq,
          'secs':ros_msg.header.stamp.secs,
          'nsecs':ros_msg.header.stamp.nsecs,
          'frame_id':str(ros_msg.header.frame_id),
          'data':ros_msg.msg.data,
          'id':ros_msg.msg.id,
          'extended':ros_msg.msg.extended,
          'dlc': ros_msg.msg.dlc}

# COMMAND ----------

display(df_dataspeed_can_msgs_CanMessageStamped)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Images

# COMMAND ----------

df_images = df_messages.where("dtype == 'sensor_msgs/Image'")
df_images.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.functions import approxCountDistinct
# MAGIC df_images.select(approxCountDistinct("record_id", rsd = 0.01)).show()

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, IntegerType, BinaryType
from pyspark.sql.functions import udf

from pyspark.sql.types import *

field = [
  StructField("record_id",LongType(), True),
  StructField("time",LongType(), True),
  StructField("topic",StringType(), True),
  StructField("dtype",StringType(), True),
  StructField("seq", LongType(), True), # part of header
  StructField("secs", LongType(), True),
  StructField("nsecs", LongType(), True),
  StructField("frame_id", StringType(), True), 
  StructField('height', IntegerType(), True), # part of message
  StructField('width', IntegerType(), True),
  StructField('encoding', StringType(), True),    
  StructField('is_bigendian', IntegerType(), True),  
  StructField('step', LongType(), True),  
  StructField('data', BinaryType(), True)
]

std_img_schema = StructType(field)
sorted_fields = sorted(std_img_schema.fields, key=lambda x: x.name)
std_img_sorted_schema = StructType(fields=sorted_fields)

def to_std_img_schema(r):

  # Step #1: deserialize the rosbag message record type
  ros_msg = msg_map_main_ros_msg(r['dtype'], r['md5sum'], r['message_definition'], r['data']['msg_raw'])
  
  # Step #2: deserialize the specific message payload & create schematized row entry
  return {'record_id':r['record_id'], 
          'time':r['time'], 
          'topic':r['topic'], 
          'dtype':r['dtype'], 
          'seq':ros_msg.header.seq,
          'secs':ros_msg.header.stamp.secs,
          'nsecs':ros_msg.header.stamp.nsecs,
          'frame_id':str(ros_msg.header.frame_id),
          'height':ros_msg.height,
          'width':ros_msg.width,
          'encoding': str(ros_msg.encoding),
          'is_bigendian':ros_msg.is_bigendian,
          'step':ros_msg.step,
          'data':ros_msg.data}

# COMMAND ----------

from pyspark.sql import Row

df_std_img = df_images.rdd.map(lambda r: Row(**to_std_img_schema(r))).toDF(std_img_sorted_schema)
df_std_img.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert ROS image data to standard byte arrays 

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.ml.image import ImageSchema

def create_image(img_id, img_in_bytes, height, width, encoding, is_bigendian):  
  import numpy as np
  import cv2
  from sparkdl.image.imageIO import imageArrayToStruct

  a = np.fromstring(str(img_in_bytes), dtype=np.uint8)
  img = a.reshape(height,width,3)
  img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

  return imageArrayToStruct(img)

create_image_udf = udf(create_image, ImageSchema.imageSchema['image'].dataType)

# COMMAND ----------

from pyspark.sql.functions import col

df_std_img2 = df_std_img \
  .withColumn("img", create_image_udf(col('record_id'), col('data'), col('height'), col('width'), col('encoding'), col('is_bigendian')))

# COMMAND ----------

display(df_std_img2.select('img', 'record_id'))

# COMMAND ----------


