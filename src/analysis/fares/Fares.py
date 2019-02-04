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

# Finds the maximum fare in an ordered, aggregated DataFrame.
def max_fares(fares_df: DataFrame, fields: Dict[str, str]):
    max_fare = fares_df.collect()[0]
    return max_fare['avg(' + fields['fare'] + ')']

# Finds the fare amount per pick-up location.
def fares_per_pickup_area(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(fields['pu_loc']).agg(avg(fields['fare'])).orderBy('avg(' + fields['fare'] + ')', ascending=False)

# Finds the fare amount per drop-off location.
def fares_per_dropoff_area(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(fields['do_loc']).agg(avg(fields['fare'])).orderBy('avg(' + fields['fare'] + ')', ascending=False)

# Finds the fare amount per hour of the day.
def fares_per_hour(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(hour(fields['do_time']).alias('hour_of_day')).agg(avg(fields['fare'])).orderBy('avg(' + fields['fare'] + ')', ascending=False)

# Finds the fare amount per day of the year.
def fares_per_day(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(dayofyear(fields['do_time']).alias('day_of_year')).agg(avg(fields['fare'])).orderBy('avg(' + fields['fare'] + ')', ascending=False)

# Finds the fare amount per month of the year.
def fares_per_month(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(month(fields['do_time']).alias('month_of_year')).agg(avg(fields['fare'])).orderBy('avg(' + fields['fare'] + ')', ascending=False)
