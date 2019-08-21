import sys

from pyspark.sql import Window, SQLContext
from pyspark.sql.functions import last

from ts.flint import *

class DataAlignmentUtil:

  def __init__(self,spark_sql_context):
    if (spark_sql_context is None) or \
      (not isinstance(spark_sql_context, SQLContext)):
      raise TypeError("spark_sql_context must be a Spark SQLContext object")

    self._flintContext = FlintContext(spark_sql_context)

  #=============================
  # BLOCK: resampling
  #=============================

  def __createTimeSeriesRDD(self, df, time_col, is_sorted, time_unit):
    return self._flintContext.read \
      .option('isSorted', is_sorted) \
      .option('timeColumn', time_col) \
      .option('timeUnit', time_unit) \
      .dataframe(df)

  def __createTimeSlicedDataFrame(self, start_time, end_time, timezone, step_size):
    return self._flintContext.read.range(start_time, end_time, timezone).clock('uniform', step_size)

  def resample(self, df, \
    time_col = 'time', timezone = 'UTC', \
    step_size = '1s', join_tolerance = '1s', 
    start_time = None, end_time = None):
    
    from pyspark.sql import functions as F  

    # if not start/end time has been specified use the 
    # min/max of the dataframe
    if start_time is None or end_time is None:
      row_start_end = df.agg(F.min('time'), F.max('time')).collect()[0]
      if start_time is None:
        start_time = row_start_end[0]
      if end_time is None:
        end_time = row_start_end[1]

    # create the time slices based on the specified duration and step size
    # create Flint's timeseries representation
    ts_timeslices = self.__createTimeSlicedDataFrame(start_time, end_time, timezone, step_size)
    ts_df = self.__createTimeSeriesRDD(df, time_col = time_col, is_sorted = True, time_unit = 'ms')

    # join the target time slices with the original dataframe
    # using a tolerance
    return ts_timeslices.leftJoin(ts_df, tolerance = join_tolerance)

  #=============================
  # BLOCK: filling 
  #=============================

  def fill(self, df, time_col, columns_to_fill, strategy):
    return strategy(df, time_col, columns_to_fill)

  def ffill_windows(self, df, time_col, columns_to_fill):    
    # define the window (and order it by time)
    window = Window.orderBy(time_col)\
                  .rowsBetween(-sys.maxsize, 0)

    # fill every column and replace columns
    for col in columns_to_fill:
      df = df.withColumn(col, last(df[col], ignorenulls=True).over(window))
    
    return df



  