from pyspark.sql.functions import col, broadcast, udf, regexp_replace, when, from_json, schema_of_json, lit, array
from pyspark.sql import Row
from pyspark.sql.types import *
from rosbagdatabricks.RosMessageLexer import RosMessageLexer
from rosbagdatabricks.RosMessageParser import RosMessageParser
from rosbagdatabricks.RosMessageParserVisitor import RosMessageParserVisitor
from rosbagdatabricks.RosMessageSchemaVisitor import RosMessageSchemaVisitor
from rosbag.bag import _get_message_type
from collections import namedtuple
from . import ROSBAG_SCHEMA
from rospy_message_converter import message_converter
from antlr4 import InputStream, CommonTokenStream
from rospy_message_converter import message_converter

import os, json, re

def read(rdd):
  df = rdd.filter(lambda r: r[1]['header'].get('op') ==  7 or 2) \
          .map(lambda r: Row(**_convert_to_row(r[0], r[1]['header'].get('op'), r[1]['header'].get('conn'), r[1]['header'], r[1]['data']))) \
          .toDF(ROSBAG_SCHEMA)

  return _denormalize_rosbag(df)

def read_topics(rdd):
  df = rdd.filter(lambda r: r[1]['header']['op'] == 7).toDF()
  return df.select('_2.header.topic').withColumn('topic', col('topic').cast('string')).distinct()

def parse(df):
  topics = df.select('topic', 'message_definition')\
           .distinct()

  columns = topics.collect()

  mock_header = StructType().add('seq', IntegerType(), True)\
                            .add('frame_id', StringType(), True)
  mock_msg = StructType().add('data', BinaryType(), True)\
                         .add('id', IntegerType(), True)\
                         .add('extended', BooleanType(), True)\
                         .add('dlc', IntegerType(), True)
  mock_struct = StructType().add('header', mock_header, True)\
                            .add('msg', mock_msg, True)  

  for column in columns:
    #struct = _convert_ros_definition_to_struct(column[1])
    struct = mock_struct
    msg_map_udf = udf(msg_map, struct)
    df = df.withColumn(column[0], 
                        when(col('topic') == column[0], 
                          msg_map_udf(col('message_definition'), 
                                      col('md5sum'), 
                                      col('dtype'), 
                                      col('data.msg_raw'))))
  return df

def msg_map(message_definition, md5sum, dtype, msg_raw):
  c = {'md5sum':md5sum, 'datatype':dtype, 'msg_def':message_definition }
  c = namedtuple('GenericDict', c.keys())(**c)

  msg_type = _get_message_type(c)
  ros_msg = msg_type()
  ros_msg.deserialize(msg_raw)

  return message_converter.convert_ros_message_to_dictionary(ros_msg)

ros_binary_types_regexp = re.compile(r'(uint8|char)\[[^\]]*\]')

def _convert_ros_definition_to_struct(message_definition):
  input_stream = InputStream(message_definition)
  lexer = RosMessageLexer(InputStream(input_stream))
  stream = CommonTokenStream(lexer)
  parser = RosMessageParser(stream)
  tree = parser.rosbag_input()
  visitor = RosMessageSchemaVisitor()
  visitor.visit(tree)

  struct = StructType()

  for field_name, field_type in visitor.fields.iteritems():
    struct.add(field_name, _convert_to_python_type(field_type), True)
  
  visitor.fields.clear()
  return struct

ros_type_to_pyspark_map = {
  'bool': 'boolean',
  'int8': 'integer',
  'uint8': 'integer',
  'int16': 'integer',
  'uint16': 'integer',
  'int32': 'integer',
  'uint32': 'integer',
  'int64': 'long',
  'uint64': 'long',
  'float32': 'float',
  'float64': 'float',
  'string': 'string'
}

def _convert_to_python_type(field_type):
  if (_is_ros_binary_type(field_type)):
    return 'binary'
  else:
    return ros_type_to_pyspark_map[field_type]

def _is_ros_binary_type(field_type):
  return re.search(ros_binary_types_regexp, field_type) is not None

def _convert_to_row(rid, opid, connid, dheader, ddata):
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
  
  return {'record_id':rid, 
          'op':opid, 
          'conn':connid, 
          'time':time, 
          'topic':topic, 
          'dtype':dtype, 
          'header':str(dheader),
          'data':result_data}

def _denormalize_rosbag(df):
  conn = df.where('op == 7') \
           .select('conn', 'dtype', 'topic', 'data.message_definition', 'data.md5sum') \
           .dropDuplicates()

  return df.where('op == 2') \
           .select('record_id', 'time', 'conn','header', 'data') \
           .join(broadcast(conn), on=['conn'])
