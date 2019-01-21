from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofyear, month

from TaxiType import TaxiType
import config

spark = SparkSession.builder\
    .master('local')\
    .appName('Tips analysis')\
    .getOrCreate()  # Create a new Spark application

path = config.path  # Specify the path of the data
filename = 'yellow_tripdata_2018-04.csv'  # Specify the name of the file
tt = TaxiType.YELLOW  # DEBUG: Type of taxi

# Initialize the correct field names - ATTENTION: field names are represented as the most recent version
if tt == TaxiType.YELLOW:
    pu_loc = 'PULocationID'
    do_loc = 'DOLocationID'
    do_time = 'tpep_dropoff_datetime'
    tip = 'tip_amount'
elif tt == TaxiType.GREEN:
    pu_loc = 'PULocationID'
    do_loc = 'DOLocationID'
    do_time = 'lpep_dropoff_datetime'
    tip = 'tip_amount'
elif tt == TaxiType.FHV:
    pu_loc = 'PULocationID'
    do_loc = 'DOLocationID'
    do_time = 'DropOff_datetime'
    tip = ''  # Tip field not present
else:
    pu_loc = ''
    do_loc = ''
    do_time = ''
    tip = ''

df = spark.read.csv(path + filename, header=True, inferSchema=True)  # Save the data into a DataFrame
# df.show()  # Show the DataFrame


# Tip amount per pick-up location
tips_per_pu = df.groupBy(pu_loc).avg(tip).orderBy('avg(' + tip + ')', ascending=False)
tips_per_pu.show(10)

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
