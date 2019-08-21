import pytest
from pyspark.sql.context import SQLContext
from timeserieshelper import dataalignment

from ts.flint import *

@pytest.fixture
def spark():
    from pyspark.sql import SparkSession
    import os
    import sys

    # ensure the PYSPARK Python version on the workers
    # matches the Python version on the driver
    # can be tricky when using virtual envs / conda
    PYSPARK_PYTHON_WORKER = 'python2'
    IS_PY2 = sys.version_info < (3,)
    if not IS_PY2:
        PYSPARK_PYTHON_WORKER = 'python3'  
    os.environ['PYSPARK_PYTHON'] = PYSPARK_PYTHON_WORKER

    spark = SparkSession.builder\
        .master('local[2]')\
        .config('spark.jars', 'lib/flint_0_6_0_databricks.jar')\
        .appName('Unit Testing')\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture
def df(spark):
    import sys
    from pyspark.sql.window import Window
    import pyspark.sql.functions as func
    import datetime

    d = [{'time': datetime.datetime(1984, 1, 1, 1, 1, 1) ,'id_2': 'test'}, \
        {'time': datetime.datetime(1984, 1, 1, 1, 1, 2), 'id': 109}, \
        {'time': datetime.datetime(1984, 1, 1, 1, 1, 3), 'id_2': 'test2'}, \
        {'time':datetime.datetime(1984, 1, 1, 1,1, 4), 'id': 110, 'id_2': 'test3'}, \
        {'time': datetime.datetime(1984, 1, 1, 1, 1, 5)},  \
        {'time': datetime.datetime(1984, 1, 1, 1, 1, 6), 'id_2': 'test4'}]
        
    return spark.createDataFrame(d)

def test_dau_initialization_no_sqlcontext_fails(spark, df):
    with pytest.raises(TypeError):
        uat = dataalignment.DataAlignmentUtil()

def test_dau_initialization_wrong_sqlcontext_type_fails(spark, df):
    with pytest.raises(TypeError):
        uat = dataalignment.DataAlignmentUtil(spark.sparkContext)

def test_fill_forwardfill_correct_number_of_null_successfully(spark, df):
    from pyspark.sql.functions import isnan, when, count, col

    uat = dataalignment.DataAlignmentUtil(SQLContext.getOrCreate(spark.sparkContext))
    result = uat.fill(df, 'time', ['id_2'], strategy=uat.ffill_windows)
    
    row_null_count_df = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    row_null_count_result = result.select([count(when(col(c).isNull(), c)).alias(c) for c in result.columns]).collect()[0]

    #debug
    df.show()
    result.show()

    assert row_null_count_result[0] == 0 # id_2 - filled
    assert row_null_count_result[0] < row_null_count_df[0] # test df has null values

    assert row_null_count_result[1] == 0 # time - nothing to fill
    assert row_null_count_result[1] == row_null_count_df[1] # test df doesn't have null values

    assert row_null_count_result[2] > 0 # id - not filled
    assert row_null_count_result[2] == row_null_count_df[2] # nulls but not filled. same like test df

def test_fill_forwardfill_df_successfully(spark, df):
    uat = dataalignment.DataAlignmentUtil(SQLContext.getOrCreate(spark.sparkContext))
    result = uat.fill(df, 'time', ['id', 'id_2'], strategy=uat.ffill_windows)
    
    #debug
    df.show()
    result.show()

    assert result.count() == df.count()

def test_fill_invalid_strategy_fails(spark, df):
    with pytest.raises(Exception):
        uat = dataalignment.DataAlignmentUtil(SQLContext.getOrCreate(spark.sparkContext))
        result = uat.fill(df, 'time', ['id', 'id_2'], strategy=None)

def test_fill_invalid_column_fails(spark, df):
    with pytest.raises(Exception):
        uat = dataalignment.DataAlignmentUtil(SQLContext.getOrCreate(spark.sparkContext))
        result = uat.fill(df, 'time', ['id', 'unknown_column'], strategy=uat.ffill_windows)

def test_resample_dataframe_with_correct_number_of_rows(spark, df):
    uat = dataalignment.DataAlignmentUtil(SQLContext.getOrCreate(spark.sparkContext))  
    result = uat.resample(df, 'time', timezone='Europe/Vienna', step_size='500ms', join_tolerance = '180ms')
 
    df.show()
    result.show()

    assert result.count() == ((df.count()) * 2) - 1

