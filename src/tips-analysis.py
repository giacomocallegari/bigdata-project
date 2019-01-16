from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofyear, month

import config

spark = SparkSession.builder\
    .master('local')\
    .appName('Tips analysis')\
    .getOrCreate()  # Create a new Spark application

path = config.path  # Specify the path of the data
filename = 'yellow_tripdata_2018-04.csv'  # Specify the name of the file

df = spark.read.csv(path + filename, header=True, inferSchema=True)  # Save the data into a DataFrame
# df.show()  # Show the DataFrame


# YELLOW TAXI only

# Tip amount per pick-up location
tips_per_pu = df.groupBy('PULocationID').avg('tip_amount').orderBy('avg(tip_amount)', ascending=False)
tips_per_pu.show(10)

# Tip amount per drop-off location
tips_per_do = df.groupBy('DOLocationID').avg('tip_amount').orderBy('avg(tip_amount)', ascending=False)
tips_per_do.show(10)

# Tip amount per drop-off time of the day
tips_per_time = df.groupBy(hour('tpep_dropoff_datetime').alias('hour_of_day')).avg('tip_amount').orderBy('avg(tip_amount)', ascending=False)
tips_per_time.show(10)

# Tip amount per drop-off day of the year
tips_per_time = df.groupBy(dayofyear('tpep_dropoff_datetime').alias('day_of_year')).avg('tip_amount').orderBy('avg(tip_amount)', ascending=False)
tips_per_time.show(10)

# Tip amount per drop-off month of the year
tips_per_time = df.groupBy(month('tpep_dropoff_datetime').alias('month_of_year')).avg('tip_amount').orderBy('avg(tip_amount)', ascending=False)
tips_per_time.show(10)
