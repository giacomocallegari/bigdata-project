from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import TaxiType
import DataReader
import MapChart
import os
import shutil

spark = SparkSession \
    .builder \
    .appName("max-trips") \
    .getOrCreate()

reader = DataReader.DataReader()
Taxi_type = TaxiType.TaxiType
reader.read_input_params()

yellow_data_path = os.path.join(os.path.dirname(__file__), "output-data/yellow-departures")
green_data_path = os.path.join(os.path.dirname(__file__), "output-data/green-departures")
fhv_data_path = os.path.join(os.path.dirname(__file__), "output-data/fhv-departures")
shp_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.shp")
dbf_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.dbf")


def create_dataframe(csv_set: list) -> DataFrame:
    return spark.read.format("csv").option("header", "true").load(csv_set)


def get_departures_by_area(taxi_df: DataFrame) -> DataFrame:
    return taxi_df.groupBy("PULocationID").count().\
        select("PULocationID", col("count").alias("Departures")).where(col("PULocationID") != "")


def max_departures(departures_by_area_df: DataFrame):
    a = departures_by_area_df.agg({"Departures": "max"}).collect()[0]
    return a["max(Departures)"]


def min_departures(departures_by_area_df: DataFrame):
    a = departures_by_area_df.agg({"Departures": "min"}).collect()[0]
    return a["min(Departures)"]


def arrival_by_area(taxy_df: DataFrame) -> DataFrame:
    return taxy_df.groupBy("DOLocationID").count().\
        orderBy("count", ascending=False).\
        select("DOLocationID", col("count").alias("Arrivals"))


yellow_chart = MapChart.MapChart()
green_chart = MapChart.MapChart()
fhv_chart = MapChart.MapChart()

if reader.type == Taxi_type.ALL:
    if len(reader.yellow_set) != 0:
        print("Processing yellow taxi,..")
        yellow_df = create_dataframe(reader.yellow_set)
        departures_by_area = get_departures_by_area(yellow_df)
        max_dep = max_departures(departures_by_area)
        if os.path.isdir(yellow_data_path):
            shutil.rmtree(yellow_data_path)
        departures_by_area.write.csv(yellow_data_path, header=False)
        arrival_by_area(yellow_df)
        yellow_chart = MapChart.MapChart(dbf_file, shp_file, yellow_data_path, 0, max_dep, "Yellow Taxi Departures")

    if len(reader.green_set) != 0:
        print("Processing green taxi...")
        green_df = create_dataframe(reader.green_set)
        departures_by_area = get_departures_by_area(green_df)
        max_dep = max_departures(departures_by_area)
        if os.path.isdir(green_data_path):
            shutil.rmtree(green_data_path)
        departures_by_area.write.csv(green_data_path, header=False)
        green_chart = MapChart.MapChart(dbf_file, shp_file, green_data_path, 0, max_dep, "Green Taxi Departures")
    if len(reader.fhv_set) != 0:
        print("Processing fhv taxi...")
        fhv_df = create_dataframe(reader.fhv_set)
        departures_by_area = get_departures_by_area(fhv_df)
        max_dep = max_departures(departures_by_area)
        if os.path.isdir(fhv_data_path):
            shutil.rmtree(fhv_data_path)
        departures_by_area.write.csv(fhv_data_path, header=False)
        fhv_chart = MapChart.MapChart(dbf_file, shp_file, fhv_data_path, 0, max_dep, "Fhv Taxi Departurews")

# Creation of chart cuncurrently
if len(reader.yellow_set) != 0 and len(reader.green_set) != 0 and len(reader.fhv_set) != 0:
    pid1 = -1
    try:
        pid1 = os.fork()
    except OSError:
        exit("Could not create child process")
    if pid1 == 0:
        pid2 = -1
        try:
            pid2 = os.fork()
        except OSError:
            exit("Could not create child process")
        if pid2 == 0:
            yellow_chart.create_chart()
            exit()
        else:
            green_chart.create_chart()
        exit()

    else:
        fhv_chart.create_chart()
elif len(reader.yellow_set) != 0 and len(reader.green_set) == 0 and len(reader.fhv_set) == 0:
    yellow_chart.create_chart()
elif len(reader.yellow_set) == 0 and len(reader.green_set) != 0 and len(reader.fhv_set) == 0:
    green_chart.create_chart()
elif len(reader.yellow_set) == 0 and len(reader.green_set) == 0 and len(reader.fhv_set) != 0:
    fhv_chart.create_chart()
elif len(reader.yellow_set) != 0 and len(reader.green_set) != 0 and len(reader.fhv_set) == 0:
    pid = -1
    try:
        pid = os.fork()
    except OSError:
        exit("Could not create child process")
    if pid == 0:
        yellow_chart.create_chart()
        exit()
    else:
        green_chart.create_chart()
elif len(reader.yellow_set) != 0 and len(reader.green_set) == 0 and len(reader.fhv_set) != 0:
    pid = -1
    try:
        pid = os.fork()
    except OSError:
        exit("Could not create child process")
    if pid == 0:
        yellow_chart.create_chart()
        exit()
    else:
        fhv_chart.create_chart()
elif len(reader.yellow_set) == 0 and len(reader.green_set) != 0 and len(reader.fhv_set) != 0:
    pid = -1
    try:
        pid = os.fork()
    except OSError:
        exit("Could not create child process")
    if pid == 0:
        green_chart.create_chart()
        exit()
    else:
        fhv_chart.create_chart()


