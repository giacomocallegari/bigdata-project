from pyspark.sql import SparkSession
import config

spark = SparkSession.builder\
    .master('local')\
    .appName('test application')\
    .getOrCreate()  # Create a new Spark application

path = config.path  # Specify the path of the data
filename = 'yellow_tripdata_2018-04.csv'  # Specify the name of the file

df = spark.read.csv(path + filename)  # Save the data into a DataFrame
df.show()  # Show the DataFrame
