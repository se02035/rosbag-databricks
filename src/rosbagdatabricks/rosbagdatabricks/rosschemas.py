from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BinaryType, LongType  

ROSBAG_SCHEMA = StructType(
  [
    StructField("conn", IntegerType(), True),
    StructField("data", StructType([
      StructField('message_definition', StringType(), True),
      StructField('md5sum', StringType(), True),
      StructField('msg_raw', BinaryType(), True)
    ])),
    StructField("dtype", StringType(), True),
    StructField("header", StringType(), True),
    StructField("op", IntegerType(), True),
    StructField("record_id", LongType(), True),
    StructField("time", LongType(), True),
    StructField("topic", StringType(), True)
  ]
)