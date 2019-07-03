# Databricks notebook source
# MAGIC %md
# MAGIC #Applying deep learning models at scale

# COMMAND ----------

# optional. use io cache to increase performance (in certain scenarios)
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

#input data
SOURCE_PARQUET = "dbfs:/mnt/rosdata/parquet_timealigned_images"

#output data
DESTINATION_PARQUET = "dbfs:/mnt/rosdata/parquet_timealigned_images_labelled"

# COMMAND ----------

image_df = spark.read.parquet(SOURCE_PARQUET)

# COMMAND ----------

from PIL import _imaging as core
from Crypto.Util import _counter
from pyspark.sql.functions import approxCountDistinct
from pyspark.ml.image import ImageSchema
from sparkdl import DeepImagePredictor

predictor = DeepImagePredictor(inputCol="img", outputCol="predicted_labels", modelName="ResNet50", decodePredictions=True, topK=10)
image_df = predictor.transform(image_df)

# COMMAND ----------

image_df.write.mode("overwrite").parquet(DESTINATION_PARQUET)
