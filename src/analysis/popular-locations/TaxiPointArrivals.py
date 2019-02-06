from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from fields_detection import init_fields
from pyspark.sql.functions import *
import TaxiType
import DataReader
import os
import shutil
import PointMapChart

class TaxiPointArrivals:
    spark = SparkSession \
        .builder \
        .appName("max-fares-hours") \
        .getOrCreate()

    yellow_data_path = os.path.join(os.path.dirname(__file__), "output-point-data/yellow-arrivals")
    green_data_path = os.path.join(os.path.dirname(__file__), "output-point-data/green-arrivals")
    fhv_data_path = os.path.join(os.path.dirname(__file__), "output-data/fhv-arrivals")
    shp_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.shp")
    dbf_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.dbf")



    def __init__(self, reader):
        self.reader = reader

    def create_dataframe(self, csv_set: list) -> DataFrame:
        return self.spark.read.format("csv").option("header", "true").load(csv_set)

    def compute_yellow(self):
        if os.path.isdir(self.yellow_data_path):
            shutil.rmtree(self.yellow_data_path)
        df = self.create_dataframe(reader.yellow_set)
        df.filter((col(fields["pu_lon"]) != 0.0) | (col(fields["do_lat"]) != 0.0)).select(fields["do_lon"], fields["do_lat"]).write.csv(self.yellow_data_path)

    def compute_green(self):
        if os.path.isdir(self.green_data_path):
            shutil.rmtree(self.green_data_path)
        df = self.create_dataframe(reader.green_set)
        df.filter((col(fields["do_lon"]) != 0.0) | (col(fields["do_lat"]) != 0.0)).select(fields["do_lon"], fields["do_lat"]).write.csv(self.green_data_path)

    def compute_fhv(self):
        if os.path.isdir(self.fhv_data_path):
            shutil.rmtree(self.fhv_data_path)
        df = self.create_dataframe(reader.green_set)
        df.filter((col(fields["do_lon"]) != 0.0) | (col(fields["do_lat"]) != 0.0)).select(fields["do_lon"], fields["do_lat"]).write.csv(self.fhv_data_path)



reader = DataReader.DataReader()  # Initialize the DataReader
reader.read_input_params()  # Read the input parameters
aa = TaxiPointArrivals(reader)  # Set the reader
Taxi_type = TaxiType.TaxiType  # Set the file type

# Initialize the correct fields for the queries
fields = init_fields(aa.reader.type, aa.reader.period)

yellow_chart = PointMapChart.MapChart(aa.dbf_file, aa.shp_file, aa.yellow_data_path)
green_chart = PointMapChart.MapChart(aa.dbf_file, aa.shp_file, aa.green_data_path)
fhv_chart = PointMapChart.MapChart(aa.dbf_file, aa.shp_file, aa.fhv_data_path)

# Perform the analysis for the desired types
if aa.reader.type == Taxi_type.ALL:
    if len(aa.reader.yellow_set) != 0:
        aa.compute_yellow()
        yellow_chart.create_chart()
    if len(aa.reader.green_set) != 0:
        aa.compute_green()
        green_chart.create_chart()
    if len(reader.fhv_set) != 0:
        aa.compute_fhv()
        green_chart.create_chart()
elif reader.type == Taxi_type.YELLOW:
    aa.compute_yellow()
    yellow_chart.create_chart()
elif reader.type == Taxi_type.GREEN:
    aa.compute_green()
    yellow_chart.create_chart()
elif reader.type == Taxi_type.FHV:
    aa.compute_fhv()
    yellow_chart.create_chart()
