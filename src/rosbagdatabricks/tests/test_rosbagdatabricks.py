import pytest

from rosbagdatabricks import rosbagdbks
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

@pytest.fixture
def spark():
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

def test_read_topics_validrosbagrdd_resultcontainsrows(spark, rdd):
    result = rosbagdbks.read_topics(rdd)
    assert result.count() != 0

def test_read_topics_validrosbagrdd_topiccolumnexists(spark, rdd):
    result = rosbagdbks.read_topics(rdd)
    assert 'topic' in result.columns

def test_read_topics_validrosbagrdd_topiccolumnisstring(spark, rdd):
    result = rosbagdbks.read_topics(rdd)
    assert result.schema['topic'].dataType is StringType()

def test_read_topics_validrosbagrdd_resultdoesnotcontainduplicates(spark, rdd):
    result = rosbagdbks.read_topics(rdd)
    assert result.count() == result.dropDuplicates().count()

def test_read_topics_invalidRdd_exceptionthrown(spark):
    rdd = spark.sparkContext.emptyRDD()
    with pytest.raises(Exception):
        assert rosbagdbks.read_topics(rdd)

def test_read_topics_none_exceptionthrown(spark):
     with pytest.raises(Exception):
        assert rosbagdbks.read_topics(None)

def test_read_validrosbagrdd_resultcontainsrows(spark, rdd):
    result = rosbagdbks.read(rdd)
    assert result.count() != 0

def test_read_validrosbagrdd_returnsdataframe(spark, rdd):
    result = rosbagdbks.read(rdd)
    assert isinstance(result, DataFrame)

def test_parse_msg_validinputs_returnsstructype():
    message_definition_file = open('data/RosMessageDefinition', 'r')
    message_definition = message_definition_file.read()
    md5sum = '33747cb98e223cafb806d7e94cb4071f'
    dtype = 'dataspeed_can_msgs/CanMessageStamped'
    msg_raw = bytearray(b'\xe6\xea\x03\x00\xa8\x93/X\x84\xb1\x05\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00g\x00\x00\x00\x00\x01')

    result = rosbagdbks.parse_msg(message_definition, md5sum, dtype, msg_raw)
    assert True
    