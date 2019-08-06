from pyspark.sql import Window
from pyspark.sql.functions import last

import sys

def fill(df, time_col, columns_to_fill, strategy):
    temp_df = df
    for col in columns_to_fill:
        filled_col = strategy(temp_df, time_col, col)
        temp_df = temp_df.withColumn(col, filled_col)

    return temp_df

def ffill_windows(df, time_col, col_to_fill):    
  # define the window
  window = Window.orderBy(time_col)\
                 .rowsBetween(-sys.maxsize, 0)

  # define the forward-filled column
  return last(df[col_to_fill], ignorenulls=True).over(window)