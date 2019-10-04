class Filler:
    
  @classmethod
  def fill(cls, df, time_col, columns_to_fill, strategy):
    """
    Fills missing values in dataframe columns using a certain
    filling strategy. 

    """
    return strategy(df, time_col, columns_to_fill)

  @classmethod
  def ffill_windows(cls, df, time_col, columns_to_fill):   
    """
    Forward filling strategy. This strategy fills empty 
    spots using the last know value of a column

    """ 
    import sys
    from pyspark.sql import Window
    from pyspark.sql.functions import last

    # define the window (and order it by time)
    window = Window.orderBy(time_col)\
                .rowsBetween(-sys.maxsize, 0)

    # fill every column and replace columns
    for col_entry in columns_to_fill:
      col_name_to_fill = col_entry[0]
      col_name_new = col_entry[1]

      if (col_name_new is None):
        col_name_new = col_name_to_fill

      df = df.withColumn(col_name_new, last(df[col_name_to_fill], ignorenulls=True).over(window))
    
    return df


class Resampler:
    
  def __init__(self,spark_sql_context):
    """
    Constructor.
    """ 
    from pyspark.sql import SQLContext
    from ts.flint import FlintContext

    if (spark_sql_context is None) or \
    (not isinstance(spark_sql_context, SQLContext)):
      raise TypeError("spark_sql_context must be a Spark SQLContext object")

    self._flintContext = FlintContext(spark_sql_context)

  def __createTimeSeriesRDD(self, df, time_col, is_sorted, time_unit):
    return self._flintContext.read \
    .option('isSorted', is_sorted) \
    .option('timeColumn', time_col) \
    .option('timeUnit', time_unit) \
    .dataframe(df)

  def __createTimeSlicedDataFrame(self, start_time, end_time, timezone, step_size):
    return self._flintContext.read.range(start_time, end_time, timezone).clock('uniform', step_size)

  def resample(self, df, \
    step_size, join_tolerance, \
    time_col = 'time', timezone = 'UTC', \
    start_time = None, end_time = None):
    """
    Resamples an existing dataframe. This can be used
    to up- or downsample the data of a dataframe. Flint is used
    to do time-based joins allowing tolerances.  

    """ 
    
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
    ts_rdd = self.__createTimeSeriesRDD(df, time_col = time_col, is_sorted = True, time_unit = 'ms')

    # join the target time slices with the original dataframe
    # using a tolerance
    return ts_timeslices.leftJoin(ts_rdd, tolerance = join_tolerance)



  