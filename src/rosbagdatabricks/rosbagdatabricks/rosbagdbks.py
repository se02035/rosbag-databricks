from pyspark.sql.functions import col, broadcast, udf, regexp_replace, when, from_json, schema_of_json, lit, array
from pyspark.sql import Row
from pyspark.sql.types import *
from rosbagdatabricks.RosMessageLexer import RosMessageLexer
from rosbagdatabricks.RosMessageParser import RosMessageParser
from rosbagdatabricks.RosMessageParserVisitor import RosMessageParserVisitor
from rosbagdatabricks.RosMessageSchemaVisitor import RosMessageSchemaVisitor
from rosbagdatabricks.RosMessageNestedSchemaVisitor import RosMessageNestedSchemaVisitor
from rosbagdatabricks.RosMessageStructVisitor import RosMessageStructVisitor
from rosbag.bag import _get_message_type
from collections import namedtuple
from . import ROSBAG_SCHEMA
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
  for column in columns:
    #file = open('output.txt', 'w')
    #file.write(column[0])
    #file.close()

    struct = convert_ros_definition_to_struct(column[1])
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


def convert_ros_definition_to_struct(message_definition):

  input_stream = InputStream(message_definition)
  lexer = RosMessageLexer(InputStream(input_stream))
  stream = CommonTokenStream(lexer)
  parser = RosMessageParser(stream)
  tree = parser.rosbag_input()

  schema_visitor = RosMessageSchemaVisitor()
  nested_schema_visitor = RosMessageNestedSchemaVisitor()

  schema = schema_visitor.visitRosbag_input(tree)
  nested_schema = nested_schema_visitor.visitRosbag_input(tree)
  
  struct = _create_struct_from_schema(schema, nested_schema)
  return struct

def _create_struct_from_schema(fields, nested_schema):
  struct = StructType()
  for field_type, field_name in fields.items():
    spark_type = _convert_to_spark_type(field_type, nested_schema)
    struct.add(field_name, spark_type, True)
  return struct

def _convert_to_spark_type(field_type, nested_schema):
  
  if _extract_type_from_arraytype(field_type) in nested_schema.keys():
    nested_fields_array = nested_schema[_extract_type_from_arraytype(field_type)]

    struct = StructType()    
    for nested_field in nested_fields_array:
      nested_field_name = nested_field.values()[0]
      nested_field_type = nested_field.keys()[0]
      nested_struct = _convert_to_spark_type(nested_field_type, nested_schema)
      struct.add(nested_field_name, nested_struct, True)
    if _is_field_type_an_array(field_type):
      return ArrayType(struct)
    else:
      return struct
  else:
    return _map_ros_type_to_struct_type(field_type)

ros_type_to_spark_type_map = {
  'bool': BooleanType,
  'int8': IntegerType,
  'uint8': IntegerType,
  'int16': IntegerType,
  'uint16': IntegerType,
  'int32': IntegerType,
  'uint32': IntegerType,
  'int64': LongType,
  'uint64': LongType,
  'float32': FloatType,
  'float64': FloatType,
  'string': StringType,
  'time': IntegerType,
  'byte': BinaryType
}

def _extract_type_from_arraytype(ros_type):
  list_brackets = re.compile(r'\[[^\]]*\]')
  return list_brackets.sub('', ros_type)

def _map_ros_type_to_struct_type(ros_type):
  if(_is_ros_binary_type(ros_type)):
    return BinaryType()
  elif (_is_field_type_an_array(ros_type)):
    list_type = _extract_type_from_arraytype(ros_type)
    return ArrayType(_map_ros_type_to_struct_type(list_type))
  else:
    return ros_type_to_spark_type_map[ros_type]()

def _is_ros_binary_type(ros_type):
  ros_binary_types_regexp = re.compile(r'(uint8|char)\[[^\]]*\]')
  return re.search(ros_binary_types_regexp, ros_type) is not None

def _is_field_type_an_array(ros_type):
  list_brackets = re.compile(r'\[[^\]]*\]')
  return list_brackets.search(ros_type) is not None

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
