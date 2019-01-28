from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofyear, month, udf

from TaxiType import TaxiType
from fields_detection import init_fields
from location_conversion import coordinates_to_zone
import config

spark = SparkSession.builder \
    .master('local') \
    .appName('Tips analysis') \
    .getOrCreate()  # Create a new Spark application


# TESTING: Change these parameters to test different cases
path = config.path  # Path of the data
filename = 'yellow_tripdata_2018-04.csv'  # Name of the file
file_type = TaxiType.YELLOW  # Type of taxi
period = '2018-04'  # Period recorded in data
sampling = 0.001  # Sampling ratio


# DATA INITIALIZATION

df = spark.read.csv(path + filename, header=True, inferSchema=True, samplingRatio=sampling)  # Save the data into a DataFrame

# Initialize the correct fields for the queries
fields = init_fields(file_type, period)
pu_loc = fields['pu_loc']
do_loc = fields['do_loc']
pu_lon = fields['pu_lon']
pu_lat = fields['pu_lat']
do_lon = fields['do_lon']
do_lat = fields['do_lat']
do_time = fields['do_time']
tip = fields['tip']

# Convert coordinates to zones if necessary
if pu_loc == '' and do_loc == '':
    pu_loc = 'pu_loc'
    do_loc = 'do_loc'

    zone_udf = udf(lambda lon, lat: coordinates_to_zone(lon, lat))

    df = df.withColumn(pu_loc, zone_udf(df[pu_lon], df[pu_lat]))  # Convert pick-up coordinates
    df = df.withColumn(do_loc, zone_udf(df[do_lon], df[do_lat]))  # Convert drop-off coordinates

# df.show()  # Show the DataFrame


# ANALYSIS

# Tip amount per pick-up location
tips_per_pu = df.groupBy(pu_loc).avg(tip).orderBy('avg(' + tip + ')', ascending=False)
tips_per_pu.show(10)

'''
# Tip amount per drop-off location
tips_per_do = df.groupBy(do_loc).avg(tip).orderBy('avg(' + tip + ')', ascending=False)
tips_per_do.show(10)

# Tip amount per drop-off time of the day
tips_per_time = df.groupBy(hour(do_time).alias('hour_of_day')).avg(tip).orderBy('avg(' + tip + ')', ascending=False)
tips_per_time.show(10)

# Tip amount per drop-off day of the year
tips_per_time = df.groupBy(dayofyear(do_time).alias('day_of_year')).avg(tip).orderBy('avg(' + tip + ')', ascending=False)
tips_per_time.show(10)

# Tip amount per drop-off month of the year
tips_per_time = df.groupBy(month(do_time).alias('month_of_year')).avg(tip).orderBy('avg(' + tip + ')', ascending=False)
tips_per_time.show(10)
'''
