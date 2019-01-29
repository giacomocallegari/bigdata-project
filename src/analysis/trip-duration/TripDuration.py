from pyspark.sql import DataFrame
from pyspark.sql.functions import *
#from pyspark.sql.functions import unix_timestamp as u_t
from typing import Dict
from location_conversion import coordinates_to_zone


# Converts coordinates to zones.
def convert_df(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    zone_udf = udf(lambda lon, lat: coordinates_to_zone(lon, lat))

    taxi_df = taxi_df.withColumn('pu_loc', zone_udf(taxi_df[fields['pu_lon']], taxi_df[fields['pu_lat']]))  # Convert pick-up coordinates
    taxi_df = taxi_df.withColumn('do_loc', zone_udf(taxi_df[fields['do_lon']], taxi_df[fields['do_lat']]))  # Convert drop-off coordinates

    return taxi_df

# Computes the duration of the trip.
def compute_duration(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    duration_udf = udf(lambda start, end: end - start)

    taxi_df = taxi_df.withColumn('duration', duration_udf(unix_timestamp(taxi_df[fields['pu_time']]), unix_timestamp(taxi_df[fields['do_time']])))  # Compute the duration of the trip

    return taxi_df

# Finds the maximum duration in an ordered, aggregated DataFrame.
def max_duration(duration_df: DataFrame, fields: Dict[str, str]):
    max_tip = duration_df.collect()[0]
    return max_tip['avg(duration)']

# Finds the trip duration per route.
def duration_per_route(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    taxi_df = compute_duration(taxi_df, fields)
    return taxi_df.groupBy(fields['pu_loc'], fields['do_loc']).agg(avg('duration')).orderBy('avg(duration)', ascending=False)

# Finds the trip duration per hour of the day; the pick-up time is considered for aggregation.
def duration_per_hour(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    taxi_df = compute_duration(taxi_df, fields)
    return taxi_df.groupBy(hour(fields['pu_time']).alias('hour_of_day')).agg(avg('duration')).orderBy('avg(duration)', ascending=False)
