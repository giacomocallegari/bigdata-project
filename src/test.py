from pyspark.sql import SparkSession
import config

spark = SparkSession.builder\
    .master('local')\
    .appName('test application')\
    .getOrCreate()  # Create a new Spark application

path = "/home/coffee/Documents/big-data/" # Specify the path of the data
filename = 'fhv_tripdata_2018.csv'  # Specify the name of the file

df = spark.read.csv(path + filename, header=True, inferSchema=True)  # Save the data into a DataFrame
df.show()  # Show the DataFrame
