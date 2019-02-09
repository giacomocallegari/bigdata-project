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

# Finds the maximum departures in an aggregated DataFrame.
def max_departures(agg_df: DataFrame) -> float:
    return float(agg_df.agg(max(agg_df[1])).collect()[0][0])

# Finds the maximum time or date in an aggregated DataFrame.
def min_time(agg_df: DataFrame) -> int:
    return int(agg_df.agg(min(agg_df[0])).collect()[0][0])

# Finds the maximum time or date in an aggregated DataFrame.
def max_time(agg_df: DataFrame) -> int:
    return int(agg_df.agg(max(agg_df[0])).collect()[0][0])

# Divides the count number by a certain factor in an aggregated DataFrame.
def divide_count(agg_df: DataFrame, factor: float) -> DataFrame:
    divide_udf = udf(lambda value: value / factor)

    div_df = agg_df.withColumn('divided_count', divide_udf(agg_df[1]))
    agg_df = div_df.select(div_df[0], div_df[2])

    return agg_df

# Finds the average departure amount per pick-up location.
def departures_per_pickup_area(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(fields['pu_loc']).agg(avg(fields['departures'])).orderBy('avg(' + fields['departures'] + ')', ascending=False)

# Finds the average departure amount per drop-off location.
def departures_per_dropoff_area(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(fields['do_loc']).agg(avg(fields['departures'])).orderBy('avg(' + fields['departures'] + ')', ascending=False)

# Finds the average departure amount per hour of the day.
def departures_per_hour(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(hour(fields['do_time']).alias('hour_of_day')).count()

# Finds the average departure amount per day of the year.
def departures_per_day(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(dayofyear(fields['do_time']).alias('day_of_year')).count()

# Finds the average departure amount per day of the week.
def departures_per_weekday(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(dayofweek(fields['do_time']).alias('day_of_week')).count()

# Finds the average departure amount per month of the year.
def departures_per_month(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(month(fields['do_time']).alias('month_of_year')).count()

# Finds the average departure amount per year.
def departures_per_year(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(year(fields['do_time']).alias('year')).count()
