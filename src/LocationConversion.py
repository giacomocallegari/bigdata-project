
import matplotlib.pyplot as plt
from pyproj import Proj, transform
from pyspark.sql.functions import *
import config
from pyspark.sql.types import BooleanType
from pyspark.sql import SparkSession
from MatrixMap import MatrixMap



accuracy = 600


spark = SparkSession \
    .builder \
    .appName("max-trips") \
    .getOrCreate()

matrixMap = MatrixMap(accuracy, "shapefile/taxi_zones.shp", "shapefile/taxi_zones.dbf")


def __convert_projection(longitude, latitude):
    in_proj = Proj('+init=EPSG:' + config.epsg_geo)  # Input projection
    out_proj = Proj('+init=EPSG:' + config.epsg_ny, preserve_units=True)  # Output projection

    # print(longitude, latitude)
    x, y = transform(in_proj, out_proj, longitude, latitude)  # Convert the coordinates
    return x, y


def get_zone_id(lon, lat):
    x, y = __convert_projection(lon, lat)
    a = matrixMap.get_neighborhood_id(x, y)
    return a


def __filter_coor(pu_lon, pu_lat, do_lon, do_lat):
    try:
        return (-74.2700 < float(pu_lon) < -71.7500) \
               and (40.4700 < float(pu_lat) < 41.3100) \
               and (-74.2700 < float(do_lon) < -71.7500) \
               and (40.4700 < float(do_lat) < 41.3100)
    except TypeError:
        return False


def convert_file(input_file_path, output_folder_path):
    get_id_udf = udf(lambda lon, lat: get_zone_id(float(lon), float(lat)))
    filter_coordinates = udf(__filter_coor, BooleanType())
    df = spark.read.format("csv").option("header", "true").load(input_file_path)
    filtered_df = df.filter(filter_coordinates(df["pickup_longitude"],df["pickup_latitude"], df["dropoff_longitude"], df["dropoff_latitude"]))
    a = filtered_df.withColumn("pu_loc", get_id_udf(filtered_df["pickup_longitude"], filtered_df["pickup_latitude"])).withColumn("do_loc", get_id_udf(filtered_df["dropoff_longitude"], filtered_df["dropoff_latitude"]))
    a.write.csv(output_folder_path)


# convert_file("/home/coffee/Downloads/taxi_data/yellow_tripdata_2010-08.csv", "bubu")

