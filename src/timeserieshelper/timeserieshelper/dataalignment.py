from pyspark.sql import Window
from pyspark.sql.functions import last
import sys

def fill(df, time_col, columns_to_fill, strategy):
  return strategy(df, time_col, columns_to_fill)

def ffill_windows(df, time_col, columns_to_fill):    
  # define the window (and order it by time)
  window = Window.orderBy(time_col)\
                 .rowsBetween(-sys.maxsize, 0)

  # fill every column and replace columns
  for col in columns_to_fill:
    df = df.withColumn(col, last(df[col], ignorenulls=True).over(window))
  
  return df