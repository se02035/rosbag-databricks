import pytest
from timeserieshelper import dataalignment as da

from pyspark.sql.context import SQLContext

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

def test_da_resampler_initialization_no_sqlcontext_fails(spark, df):
    with pytest.raises(TypeError):
        uat = da.Resampler()

def test_da_resampler_initialization_wrong_sqlcontext_type_fails(spark, df):
    with pytest.raises(TypeError):
        uat = da.Resampler(spark.sparkContext)

def test_da_resampler_resample_dataframe_with_correct_number_of_rows(spark, df):
    uat = da.Resampler(SQLContext.getOrCreate(spark.sparkContext))  
    result = uat.resample(df, time_col='time', timezone='Europe/Vienna', step_size='500ms', join_tolerance = '180ms')

    df.show()
    result.show()
    
    assert result.count() == ((df.count()) * 2) - 1