from pyspark.sql.functions import col, broadcast
from pyspark.sql import Row
from pyspark.sql.types import StructType
from rosbagdatabricks.RosMessageLexer import RosMessageLexer
from rosbagdatabricks.RosMessageParser import RosMessageParser
from rosbagdatabricks.RosMessageParserVisitor import RosMessageParserVisitor
from rosbagdatabricks.RosMessageSchemaVisitor import RosMessageSchemaVisitor
from rosbag.bag import _get_message_type
from collections import namedtuple
from . import ROSBAG_SCHEMA
from antlr4 import *

import os

def read(rdd):
  df = rdd.filter(lambda r: r[1]['header'].get('op') ==  7 or 2) \
          .map(lambda r: Row(**_convert_to_row(r[0], r[1]['header'].get('op'), r[1]['header'].get('conn'), r[1]['header'], r[1]['data']))) \
          .toDF(ROSBAG_SCHEMA)

  return _denormalize_rosbag(df)

def read_topics(rdd):
  df = rdd.filter(lambda r: r[1]['header']['op'] == 7).toDF()
  return df.select('_2.header.topic').withColumn('topic', col('topic').cast('string')).distinct()

def parse_msg(message_definition, md5sum, dtype, msg_raw):
  struct = _generate_struct(message_definition)
  msg = _msg_map(message_definition, md5sum, dtype, msg_raw)


  return msg

def _msg_map(message_definition, md5sum, dtype, msg_raw):
  c = {'md5sum':md5sum, 'datatype':dtype, 'msg_def':message_definition }
  c = namedtuple('GenericDict', c.keys())(**c)
  
  msg_type = _get_message_type(c)
  msg = msg_type()
  msg.deserialize(msg_raw)
  return msg

def _generate_struct(message_definition):
  lexer = RosMessageLexer(InputStream(message_definition))
  stream = CommonTokenStream(lexer)
  parser = RosMessageParser(stream)
  tree = parser.rosbag_input()
  visitor = RosMessageSchemaVisitor()
  visitor.visit(tree)

  struct_fields = []
  for f in visitor.fields:
       struct_fields.append({'metadata': {}, 'name': f[0], 'nullable': True, 'type': 'integer'})

  schema_dict = {
    'fields': struct_fields, 
    'type': 'struct'
  }
  return StructType.fromJson(schema_dict)

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
