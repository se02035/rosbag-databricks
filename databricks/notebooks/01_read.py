# Databricks notebook source
from rosbagdatabricks import rosbag

import os

# COMMAND ----------

bagFilePath = 'dbfs:/mnt/datasets/rosbag/0.1sec.bag'
idxFilePath = 'dbfs:/mnt/datasets/rosbag/0.1sec.bag.idx.bin'

sc.addFile(idxFilePath)

rdd = sc.newAPIHadoopFile(
  path = bagFilePath,
  inputFormatClass = 'de.valtech.foss.RosbagMapInputFormat',
  keyClass = 'org.apache.hadoop.io.LongWritable',
  valueClass = 'org.apache.hadoop.io.MapWritable',
  conf = {'RosbagInputFormat.chunkIdx': os.path.basename(idxFilePath)}
)
  
df = rosbag.read(rdd)