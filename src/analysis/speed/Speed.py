from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import Dict
from location_conversion import coordinates_to_zone


# Converts coordinates to zones.
def convert_df(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    zone_udf = udf(lambda lon, lat: coordinates_to_zone(lon, lat))

    taxi_df = taxi_df.withColumn('pu_loc', zone_udf(taxi_df[fields['pu_lon']], taxi_df[fields['pu_lat']]))  # Convert pick-up coordinates
    taxi_df = taxi_df.withColumn('do_loc', zone_udf(taxi_df[fields['do_lon']], taxi_df[fields['do_lat']]))  # Convert drop-off coordinates

    return taxi_df

# Computes the duration of each trip.
def compute_duration(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    duration_udf = udf(lambda start, end: float(end) - float(start))

    taxi_df = taxi_df.withColumn('unix_pu', unix_timestamp(taxi_df[fields['pu_time']])).withColumn('unix_do', unix_timestamp(taxi_df[fields['do_time']]))
    taxi_df = taxi_df.filter((taxi_df['unix_pu'].isNotNull()) & (taxi_df['unix_do']).isNotNull())
    # taxi_df.filter(taxi_df['unix_pu'].contains('2018') | taxi_df['unix_do'].contains('2018')).show()

    taxi_df = taxi_df.withColumn('duration', duration_udf(taxi_df['unix_pu'], taxi_df['unix_do']))  # Compute the duration of the trip

    return taxi_df

# Computes the average speed of each trip.
def compute_speed(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    speed_udf = udf(lambda distance, duration: 3600 * float(distance) / float(duration))

    taxi_df = compute_duration(taxi_df, fields)
    taxi_df = taxi_df.filter(taxi_df['duration'] > 0)  # Ignore trips with null duration
    taxi_df.show()
    taxi_df = taxi_df.withColumn('speed', speed_udf(taxi_df[fields['dist']], taxi_df['duration']))  # Compute the average speed of the trip

    return taxi_df

# Finds the maximum average speed in an aggregated DataFrame.
def max_speed(agg_df: DataFrame) -> float:
    return float(agg_df.agg(max(agg_df[1])).collect()[0][0])

# Finds the maximum time or date in an aggregated DataFrame.
def min_time(agg_df: DataFrame) -> int:
    return int(agg_df.agg(min(agg_df[0])).collect()[0][0])

# Finds the maximum time or date in an aggregated DataFrame.
def max_time(agg_df: DataFrame) -> int:
    return int(agg_df.agg(max(agg_df[0])).collect()[0][0])

'''
# Finds the average speed amount per pick-up location.
def speed_per_pickup_area(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(fields['pu_loc']).agg(avg(fields['speed'])).orderBy('avg(' + fields['speed'] + ')', ascending=False)

# Finds the average speed amount per drop-off location.
def speed_per_dropoff_area(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(fields['do_loc']).agg(avg(fields['speed'])).orderBy('avg(' + fields['speed'] + ')', ascending=False)
'''

# Finds the average speed per hour of the day.
def speed_per_hour(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    taxi_df = compute_speed(taxi_df, fields)
    return taxi_df.groupBy(hour(fields['do_time']).alias('hour_of_day')).agg(avg('speed'))

# Finds the average speed per day of the year.
def speed_per_day(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    taxi_df = compute_speed(taxi_df, fields)
    return taxi_df.groupBy(dayofyear(fields['do_time']).alias('day_of_year')).agg(avg('speed'))

# Finds the average speed per day of the week.
def speed_per_weekday(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    taxi_df = compute_speed(taxi_df, fields)
    return taxi_df.groupBy(dayofweek(fields['do_time']).alias('day_of_week')).agg(avg('speed'))

# Finds the average speed per month of the year.
def speed_per_month(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    taxi_df = compute_speed(taxi_df, fields)
    return taxi_df.groupBy(month(fields['do_time']).alias('month_of_year')).agg(avg('speed'))

# Finds the average speed per year.
def speed_per_year(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    taxi_df = compute_speed(taxi_df, fields)
    return taxi_df.groupBy(year(fields['do_time']).alias('year')).agg(avg('speed'))
