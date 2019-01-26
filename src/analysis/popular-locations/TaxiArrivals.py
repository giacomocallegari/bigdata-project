from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import TaxiType
import DataReader
import MapChart
import os
import shutil


class TaxiArrivals:
    spark = SparkSession \
        .builder \
        .appName("max-trips") \
        .getOrCreate()

    yellow_data_path = os.path.join(os.path.dirname(__file__), "output-data/yellow-arrivals")
    green_data_path = os.path.join(os.path.dirname(__file__), "output-data/green-arrivals")
    fhv_data_path = os.path.join(os.path.dirname(__file__), "output-data/fhv-arrivals")
    shp_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.shp")
    dbf_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.dbf")

    yellow_chart = MapChart.MapChart()
    green_chart = MapChart.MapChart()
    fhv_chart = MapChart.MapChart()

    def __init__(self, reader):
        self.reader = reader

    def create_dataframe(self, csv_set: list) -> DataFrame:
        return self.spark.read.format("csv").option("header", "true").load(csv_set)

    def get_arrivals_by_area(self, taxi_df: DataFrame) -> DataFrame:
        return taxi_df.groupBy("DOLocationID").count().\
            select("DOLocationID", col("count").alias("Arrivals")).where((col("DOLocationID") != "") & (col("DOLocationID") < 264))

    def max_arrivals(self, arrivals_by_area_df: DataFrame):
        a = arrivals_by_area_df.agg({"Arrivals": "max"}).collect()[0]
        return a["max(Arrivals)"]

    def min_arrivals(self, arrivals_by_area_df: DataFrame):
        a = arrivals_by_area_df.agg({"Arrivals": "min"}).collect()[0]
        return a["min(Arrivals)"]

    def show_charts(self):
        # Creation of chart cuncurrently
        if len(self.reader.yellow_set) != 0 and len(self.reader.green_set) != 0 and len(self.reader.fhv_set) != 0:
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
                    self.yellow_chart.create_chart()
                    exit()
                else:
                    self.green_chart.create_chart()
                exit()

            else:
                self.fhv_chart.create_chart()
        elif len(self.reader.yellow_set) != 0 and len(self.reader.green_set) == 0 and len(self.reader.fhv_set) == 0:
            self.yellow_chart.create_chart()
        elif len(self.reader.yellow_set) == 0 and len(self.reader.green_set) != 0 and len(self.reader.fhv_set) == 0:
            self.green_chart.create_chart()
        elif len(self.reader.yellow_set) == 0 and len(self.reader.green_set) == 0 and len(self.reader.fhv_set) != 0:
            self.fhv_chart.create_chart()
        elif len(self.reader.yellow_set) != 0 and len(self.reader.green_set) != 0 and len(self.reader.fhv_set) == 0:
            pid = -1
            try:
                pid = os.fork()
            except OSError:
                exit("Could not create child process")
            if pid == 0:
                self.yellow_chart.create_chart()
                exit()
            else:
                self.green_chart.create_chart()
        elif len(self.reader.yellow_set) != 0 and len(self.reader.green_set) == 0 and len(self.reader.fhv_set) != 0:
            pid = -1
            try:
                pid = os.fork()
            except OSError:
                exit("Could not create child process")
            if pid == 0:
                self.yellow_chart.create_chart()
                exit()
            else:
                self.fhv_chart.create_chart()
        elif len(self.reader.yellow_set) == 0 and len(self.reader.green_set) != 0 and len(self.reader.fhv_set) != 0:
            pid = -1
            try:
                pid = os.fork()
            except OSError:
                exit("Could not create child process")
            if pid == 0:
                self.green_chart.create_chart()
                exit()
            else:
                self.fhv_chart.create_chart()

    def compute_yellow(self):
        print("Processing yellow taxi,..")
        yellow_df = self.create_dataframe(self.reader.yellow_set)
        arrivals_by_area = self.get_arrivals_by_area(yellow_df)
        max_arr = self.max_arrivals(arrivals_by_area)
        if os.path.isdir(self.yellow_data_path):
            shutil.rmtree(self.yellow_data_path)
        arrivals_by_area.write.csv(self.yellow_data_path, header=False)
        self.yellow_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.yellow_data_path, 0, max_arr, "Yellow Taxi Arrivals")

    def compute_green(self):
        print("Processing green taxi...")
        green_df = self.create_dataframe(self.reader.green_set)
        arrivals_by_area = self.get_arrivals_by_area(green_df)
        max_arr = self.max_arrivals(arrivals_by_area)
        if os.path.isdir(self.green_data_path):
            shutil.rmtree(self.green_data_path)
        arrivals_by_area.write.csv(self.green_data_path, header=False)
        self.green_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.green_data_path, 0, max_arr, "Green Taxi Arrivals")

    def compute_fhv(self):
        print("Processing fhv taxi...")
        fhv_df = self.create_dataframe(self.reader.fhv_set)
        arrivals_by_area = self.get_arrivals_by_area(fhv_df)
        max_arr = self.max_arrivals(arrivals_by_area)
        if os.path.isdir(self.fhv_data_path):
            shutil.rmtree(self.fhv_data_path)
        arrivals_by_area.write.csv(self.fhv_data_path, header=False)
        self.fhv_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.fhv_data_path, 0, max_arr, "Fhv Taxi Arrivals")


reader = DataReader.DataReader()
reader.read_input_params()
aa = TaxiArrivals(reader)
Taxi_type = TaxiType.TaxiType

if aa.reader.type == Taxi_type.ALL:
    if len(aa.reader.yellow_set) != 0:
        aa.compute_yellow()
    if len(aa.reader.green_set) != 0:
        aa.compute_green()
    if len(reader.fhv_set) != 0:
        aa.compute_fhv()
    aa.show_charts()
elif reader.type == Taxi_type.YELLOW:
    aa.compute_yellow()
    aa.yellow_chart.create_chart()
elif reader.type == Taxi_type.FHV:
    aa.compute_fhv()
    aa.fhv_chart.create_chart()
elif reader.type == Taxi_type.GREEN:
    aa.compute_green()
    aa.green_chart.create_chart()
