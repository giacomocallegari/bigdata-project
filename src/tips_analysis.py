from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofyear, month

from TaxiType import TaxiType
from fields_detection import init_fields
import config

spark = SparkSession.builder\
    .master('local')\
    .appName('Tips analysis')\
    .getOrCreate()  # Create a new Spark application


path = config.path  # DEBUG: Path of the data
filename = 'yellow_tripdata_2018-04.csv'  # DEBUG: Name of the file
file_type = TaxiType.YELLOW  # DEBUG: Type of taxi
period = '2018-04'  # DEBUG: Period recorded in data

df = spark.read.csv(path + filename, header=True, inferSchema=True, samplingRatio=0.001)  # Save the data into a DataFrame
df_columns = df.schema.names
# df.show()  # Show the DataFrame

# Initialize the correct fields for the queries
fields = init_fields(file_type, period)
pu_loc = fields['pu_loc']
do_loc = fields['do_loc']
do_time = fields['do_time']
tip = fields['tip']


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
