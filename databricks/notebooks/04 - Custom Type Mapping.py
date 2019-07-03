# Databricks notebook source
# optional. use io cache to increase performance (in certain scenarios)
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

#input data
SOURCE_PARQUET = "dbfs:/mnt/rosdata/parquet_timealigned"

#output data
DESTINATION_PARQUET = "dbfs:/mnt/rosdata/parquet_timealigned_images"

# COMMAND ----------

df_data = spark.read.parquet(SOURCE_PARQUET)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Images

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.ml.image import ImageSchema
import base64

#===========================================
# ATTENTION
#===========================================
# has to be in Python2!! Python3 (np.fromstring) produces different results (larger array)
#===========================================
def create_image(img_in_bytes, height, width, encoding, is_bigendian):  
  import numpy as np
  import cv2
  from sparkdl.image.imageIO import imageArrayToStruct

  result = None
  
  # potentially, due to the time alignment there area image entries with empty data
  if (img_in_bytes):

    a = np.fromstring(base64.standard_b64decode(img_in_bytes), dtype=np.uint8)
    img = a.reshape(height,width,3)
    img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
    result = imageArrayToStruct(img)
  
  return result

create_image_udf = udf(create_image, ImageSchema.imageSchema['image'].dataType)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

col_name = "/center_camera/image_color"
df_data = df_data \
  .withColumn(col_name, create_image_udf(col(col_name + '.data'), col(col_name + '.height').cast(IntegerType()), col(col_name + '.width').cast(IntegerType()), col(col_name + '.encoding'), col(col_name + '.is_bigendian')))

# COMMAND ----------

df_data.write.mode("overwrite").parquet(DESTINATION_PARQUET)
