import pytest
import os

from rosbagdatabricks import rosbagdbks
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, when

@pytest.fixture
def spark():
    os.environ['PYSPARK_PYTHON'] = '/home/florian/anaconda3/envs/rosbag/bin/python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/florian/anaconda3/envs/rosbag/bin/python'
    
    from pyspark.sql import SparkSession

    spark = SparkSession.builder\
        .master('local[2]')\
        .config('spark.jars', 'lib/protobuf-java-3.3.0.jar,lib/rosbaginputformat_2.11-0.9.8.jar,lib/scala-library-2.11.8.jar')\
        .appName('Unit Testing')\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture
def rdd(spark):
    return spark.sparkContext.newAPIHadoopFile(
        path = 'data/0.1sec.bag',
        inputFormatClass = 'de.valtech.foss.RosbagMapInputFormat',
        keyClass = 'org.apache.hadoop.io.LongWritable',
        valueClass = 'org.apache.hadoop.io.MapWritable',
        conf = {'RosbagInputFormat.chunkIdx':'data/0.1sec.bag.idx.bin'}
    )

#def test_read_topics_validrosbagrdd_resultcontainsrows(spark, rdd):
#    result = rosbagdbks.read_topics(rdd)
#    assert result.count() != 0
#
#def test_read_topics_validrosbagrdd_topiccolumnexists(spark, rdd):
#    result = rosbagdbks.read_topics(rdd)
#    assert 'topic' in result.columns
#
#def test_read_topics_validrosbagrdd_topiccolumnisstring(spark, rdd):
#    result = rosbagdbks.read_topics(rdd)
#    assert result.schema['topic'].dataType is StringType()
#
#def test_read_topics_validrosbagrdd_resultdoesnotcontainduplicates(spark, rdd):
#    result = rosbagdbks.read_topics(rdd)
#    assert result.count() == result.dropDuplicates().count()
#
#def test_read_topics_invalidRdd_exceptionthrown(spark):
#    rdd = spark.sparkContext.emptyRDD()
#    with pytest.raises(Exception):
#        assert rosbagdbks.read_topics(rdd)
#
#def test_read_topics_none_exceptionthrown(spark):
#     with pytest.raises(Exception):
#        assert rosbagdbks.read_topics(None)
#
#def test_read_validrosbagrdd_resultcontainsrows(spark, rdd):
#    result = rosbagdbks.read(rdd)
#    assert result.count() != 0
#
#def test_read_validrosbagrdd_returnsdataframe(spark, rdd):
#    result = rosbagdbks.read(rdd)
#    assert isinstance(result, DataFrame)
#
#def test_parse_validinputs_resultisnotempty(spark):
#   df = spark.read.parquet('data/0.1sec.parquet').limit(10)
#   result = rosbagdbks.parse(df)
#   result.collect()
#   assert result.count() != 0
#   assert result.filter('`/can_bus_dbw/can_rx`.msg.dlc is not null').count() > 0
#   #assert result.filter('`/can_bus_dbw/can_rx`.header.stamp.sec is not null').count() > 0
#
#def test_parse_camera_data_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/left_camera/image_color/compressed'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#
#def test_parse_validinputs_resultstructiscomplete(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').limit(10)
#
#    result = rosbagdbks.parse(df)
#    result.collect()
#
#    assert len(result.select('/can_bus_dbw/can_rx.*').columns) == 2
#
#def test_msg_map_validinputs_resultisnotnull():
#    message_definition_file = open('data/RosMessageDefinition','r')
#    message_definition = message_definition_file.read()
#    md5sum = '33747cb98e223cafb806d7e94cb4071f'
#    dtype = 'dataspeed_can_msgs/CanMessageStamped'
#    msg_raw = bytearray(b'\xe6\xea\x03\x00\xa8\x93/X\x84\xb1\x05\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00g\x00\x00\x00\x00\x01')
#
#    result = rosbagdbks.msg_map(message_definition, md5sum, dtype, msg_raw)
#
#    assert result != None
#
#def test_msg_map_gearreport_resultisnotnull():
#    message_definition_file = open('data/RosMessageDefinition_GearReport','r')
#    message_definition = message_definition_file.read()
#    md5sum = 'f33342dfeb80c29d8fe4b31e22519594'
#    dtype = 'dbw_mkz_msgs/GearReport'
#    msg_raw = bytearray(b'6\x1c\x00\x00\xa8\x93/X\x84\xb1\x05\x00\x00\x00\x00\x00\x04\x00\x01\x00')
#
#    result = rosbagdbks.msg_map(message_definition, md5sum, dtype, msg_raw)
#
#    assert result != None
#
#def test_convert_ros_definition_to_struct_flatdefinition_hasfields():
#    message_definition_file = open('data/RosMessageDefinition','r')
#    message_definition = message_definition_file.read()
#
#    result = rosbagdbks.convert_ros_definition_to_struct(message_definition)
#
#    assert result.fieldNames()
#    assert 'msg' in result.fieldNames()
#    assert 'header' in result.fieldNames()
#
#def test_convert_ros_definition_to_struct_nesteddefinition_hasfields():
#    message_definition_file = open('data/RosMessageDefinition_Nested','r')
#    message_definition = message_definition_file.read()
#
#    result = rosbagdbks.convert_ros_definition_to_struct(message_definition)
#
#    assert result.fieldNames()
#    assert 'twist' in result.fieldNames()
#    assert 'header' in result.fieldNames()
#
#def test_convert_ros_definition_to_struct_vectordefinition_hasfields():
#    message_definition_file = open('data/RosMessageDefinition_Vector','r')
#    message_definition = message_definition_file.read()
#
#    result = rosbagdbks.convert_ros_definition_to_struct(message_definition)
#
#    assert result.fieldNames()
#    assert 'packets' in result.fieldNames()
#    assert 'header' in result.fieldNames()
#

def test_convert_ros_definition_to_struct_diagnosticsdefinition_hasfields():
   message_definition_file = open('data/RosMessageDefinition_Diagnostics','r')
   message_definition = message_definition_file.read()

   result = rosbagdbks.convert_ros_definition_to_struct(message_definition)

   assert result.fieldNames()
   assert 'header' in result.fieldNames()


### Test all sensors
#def test_parse_vehicle_suspension_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/suspension_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_filtered_accel_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/filtered_accel'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_brake_info_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/brake_info_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_throttle_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/throttle_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_center_camera_image_color_compressed_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/center_camera/image_color/compressed'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_misc_1_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/misc_1_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_right_camera_camera_info_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/right_camera/camera_info'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_sonar_cloud_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/sonar_cloud'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_time_reference_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/time_reference'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_gps_vel_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/gps/vel'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_imu_data_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/imu/data'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_fuel_level_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/fuel_level_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_fix_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/fix'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_left_camera_camera_info_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/left_camera/camera_info'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_velodyne_packets_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/velodyne_packets'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_pressure_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/pressure'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_can_bus_dbw_can_rx_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/can_bus_dbw/can_rx'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_left_camera_image_color_compressed_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/left_camera/image_color/compressed'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_imu_data_raw_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/imu/data_raw'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_gps_fix_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/gps/fix'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_steering_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/steering_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_tire_pressure_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/tire_pressure_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_brake_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/brake_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_center_camera_camera_info_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/center_camera/camera_info'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_wheel_speed_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/wheel_speed_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_right_camera_image_color_compressed_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/right_camera/image_color/compressed'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
# def test_parse_diagnostics_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet')
#    df = df.filter((df.topic=='/diagnostics') | (df.topic == '/pressure'))
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
   
#def test_parse_vehicle_gps_time_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/gps/time'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_joint_states_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/joint_states'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_throttle_info_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/throttle_info_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_surround_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/surround_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_vehicle_gear_report_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/vehicle/gear_report'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#    
#def test_parse_ecef__resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic=='/ecef/'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0
#

# def test_parse_all_except_diag_data_resultisnotempty(spark):
#    df = spark.read.parquet('data/0.1sec.parquet').filter("topic != '/diagnostics'")
#    result = rosbagdbks.parse(df)
#    result.collect()
#    assert result.count() != 0

def test_parse_all_data_resultisnotempty(spark):
   df = spark.read.parquet('data/0.1sec.parquet')
   result = rosbagdbks.parse(df)
   result.collect()
   assert result.count() != 0

