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

# Finds the maximum tip in an ordered, aggregated DataFrame.
def max_tips(tips_df: DataFrame, fields: Dict[str, str]):
    max_tip = tips_df.collect()[0]
    return max_tip['avg(' + fields['tip'] + ')']

# Finds the tip amount per pick-up location.
def tips_per_pickup_area(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(fields['pu_loc']).agg(avg(fields['tip'])).orderBy('avg(' + fields['tip'] + ')', ascending=False)

# Finds the tip amount per drop-off location.
def tips_per_dropoff_area(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(fields['do_loc']).agg(avg(fields['tip'])).orderBy('avg(' + fields['tip'] + ')', ascending=False)

# Finds the tip amount per hour of the day.
def tips_per_hour(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(hour(fields['do_time']).alias('hour_of_day')).agg(avg(fields['tip'])).orderBy('avg(' + fields['tip'] + ')', ascending=False)

# Finds the tip amount per day of the year.
def tips_per_day(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(dayofyear(fields['do_time']).alias('day_of_year')).agg(avg(fields['tip'])).orderBy('avg(' + fields['tip'] + ')', ascending=False)

# Finds the tip amount per month of the year.
def tips_per_month(taxi_df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return taxi_df.groupBy(month(fields['do_time']).alias('month_of_year')).agg(avg(fields['tip'])).orderBy('avg(' + fields['tip'] + ')', ascending=False)
