import pytest
from timeserieshelper import dataalignment

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
        .appName('Unit Testing')\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@pytest.fixture
def df(spark):
    import sys
    from pyspark.sql.window import Window
    import pyspark.sql.functions as func

    d = [{'ts': 1, 'id_2': 'test'}, {'ts': 2, 'id': 109}, {'ts': 3, 'id_2': 'test2'}, {'ts': 4, 'id': 110, 'id_2': 'test3'}, {'ts': 5},  {'ts': 6, 'id_2': 'test4'}]
    return spark.createDataFrame(d)

def test_forwardfill_df_successfully(spark, df):
    result = dataalignment.fill(df, 'ts', ['id', 'id_2'], strategy=dataalignment.ffill_windows)
    
    #debug
    df.show()
    result.show()

    assert result.count() == df.count()



