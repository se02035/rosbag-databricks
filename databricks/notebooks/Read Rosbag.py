# Databricks notebook source
dbutils.fs.cp('dbfs:/mnt/datasets/rosbag/data.bag', 'file:/tmp/data.bag')

# COMMAND ----------

dbutils.fs.cp('dbfs:/mnt/datasets/rosbag/lib/protobuf-java-3.3.0.jar', 'file:/tmp/lib/protobuf-java-3.3.0.jar')
dbutils.fs.cp('dbfs:/mnt/datasets/rosbag/lib/rosbaginputformat_2.11-0.9.5.jar', 'file:/tmp/lib/rosbaginputformat_2.11-0.9.5.jar')
dbutils.fs.cp('dbfs:/mnt/datasets/rosbag/lib/scala-library-2.11.8.jar', 'file:/tmp/lib/scala-library-2.11.8.jar')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC java -jar /tmp/lib/rosbaginputformat_2.11-0.9.5.jar -f /tmp/data.bag

# COMMAND ----------

dbutils.fs.cp('file:/tmp/data.bag.idx.bin', 'dbfs:/mnt/datasets/rosbag/data.bag.idx.bin')

# COMMAND ----------



# COMMAND ----------

sc.addFile('dbfs:/mnt/datasets/rosbag/cal_loop.bag.idx.bin')

# COMMAND ----------

fin = sc.newAPIHadoopFile(
    path =             'dbfs:/mnt/datasets/rosbag/cal_loop.bag',
    inputFormatClass = 'de.valtech.foss.RosbagMapInputFormat',
    keyClass =         'org.apache.hadoop.io.LongWritable',
    valueClass =       'org.apache.hadoop.io.MapWritable',
    conf =             {'RosbagInputFormat.chunkIdx':'cal_loop.bag.idx.bin'})

# COMMAND ----------

from pyspark.sql.functions import col

headerRdd = fin.filter(lambda r: r[1]['header']['op'] == 7)

headerDf = headerRdd.toDF()
headerDf.cache()

# COMMAND ----------

topics = headerDf.select('_2.header.topic', '_2.header.conn').withColumn('topic', col('topic').cast('string')).distinct()
display(topics)

# COMMAND ----------

def msg_map(r, func=str, conn={}):
    from collections import namedtuple
    from rosbag.bag import _get_message_type
    if r[1]['header']['op'] == 2 and r[1]['header']['conn'] == conn['header']['conn']:
        c = conn['data']
        c['datatype'] = str(c['type'])
        c['msg_def'] = str(c['message_definition'])
        c['md5sum'] = str(c['md5sum'])
        c = namedtuple('GenericDict', c.keys())(**c)
        msg_type = _get_message_type(c)
        msg = msg_type()
        msg.deserialize(r[1]['data'])
        yield func(msg)

# COMMAND ----------

from functools import partial

rdd = fin.flatMap(
  partial(msg_map, conn=conn_d['/vehicle/steering_report'])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Images

# COMMAND ----------

#from PIL import Image
#from io import BytesIO
#
#rdd = fin.flatMap(
#  partial(msg_map, func=lambda r: r.data, conn=conn_d['/cc'])
#)
#arr = rdd.take(1)[0]
#
#Image.open(BytesIO(arr))

# COMMAND ----------

from PIL import Image
from io import BytesIO
from pyspark.sql.types import BinaryType, StructType, StructField
from functools import partial

rdd = fin.flatMap(
    partial(msg_map, func=lambda r: r.data, conn=conn_d['/center_camera/image_color/compressed'])
)

rddTuple = rdd.map(lambda x: (bytearray(x),))
schema = StructType([StructField('rawdata', BinaryType(), False)])
df = rddTuple.toDF(schema)
df.cache()

# COMMAND ----------

from sparkdl.image.imageIO import PIL_decode, imageArrayToStruct
from pyspark.sql.functions import col
from pyspark.ml.image import ImageSchema

imageUdf = udf(lambda b: imageArrayToStruct(PIL_decode(b)), ImageSchema.imageSchema['image'].dataType)


img = df.withColumn('image', imageUdf(col('rawdata')))
display(img.select('image'))