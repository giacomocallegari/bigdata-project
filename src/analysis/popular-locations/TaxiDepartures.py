from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import TaxiType
import DataReader
import MapChart
import os
import shutil


class TaxiDepartures:
    spark = SparkSession \
        .builder \
        .appName("max-trips") \
        .getOrCreate()

    yellow_data_path = os.path.join(os.path.dirname(__file__), "output-data/yellow-departures")
    green_data_path = os.path.join(os.path.dirname(__file__), "output-data/green-departures")
    fhv_data_path = os.path.join(os.path.dirname(__file__), "output-data/fhv-departures")
    shp_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.shp")
    dbf_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.dbf")

    yellow_chart = MapChart.MapChart()
    green_chart = MapChart.MapChart()
    fhv_chart = MapChart.MapChart()

    def __init__(self, reader):
        self.reader = reader

    def create_dataframe(self, csv_set: list) -> DataFrame:
        return self.spark.read.format("csv").option("header", "true").load(csv_set)

    def get_departures_by_area(self, taxi_df: DataFrame) -> DataFrame:
        return taxi_df.groupBy("PULocationID").count().\
            select("PULocationID", col("count").alias("Departures")).where(col("PULocationID") != "")

    def max_departures(self, departures_by_area_df: DataFrame):
        a = departures_by_area_df.agg({"Departures": "max"}).collect()[0]
        return a["max(Departures)"]

    def min_departures(self, departures_by_area_df: DataFrame):
        a = departures_by_area_df.agg({"Departures": "min"}).collect()[0]
        return a["min(Departures)"]

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
        departures_by_area = self.get_departures_by_area(yellow_df)
        max_dep = self.max_departures(departures_by_area)
        if os.path.isdir(self.yellow_data_path):
            shutil.rmtree(self.yellow_data_path)
        departures_by_area.write.csv(self.yellow_data_path, header=False)
        self.yellow_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.yellow_data_path, 0, max_dep, "Yellow Taxi Departures")

    def compute_green(self):
        print("Processing green taxi...")
        green_df = self.create_dataframe(self.reader.green_set)
        departures_by_area = self.get_departures_by_area(green_df)
        max_dep = self.max_departures(departures_by_area)
        if os.path.isdir(self.green_data_path):
            shutil.rmtree(self.green_data_path)
        departures_by_area.write.csv(self.green_data_path, header=False)
        self.green_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.green_data_path, 0, max_dep, "Green Taxi Departures")

    def compute_fhv(self):
        print("Processing fhv taxi...")
        fhv_df = self.create_dataframe(self.reader.fhv_set)
        departures_by_area = self.get_departures_by_area(fhv_df)
        max_dep = self.max_departures(departures_by_area)
        if os.path.isdir(self.fhv_data_path):
            shutil.rmtree(self.fhv_data_path)
        departures_by_area.write.csv(self.fhv_data_path, header=False)
        self.fhv_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.fhv_data_path, 0, max_dep, "Fhv Taxi Departurews")


reader = DataReader.DataReader()
reader.read_input_params()
da = TaxiDepartures(reader)
Taxi_type = TaxiType.TaxiType

if da.reader.type == Taxi_type.ALL:
    if len(da.reader.yellow_set) != 0:
        da.compute_yellow()
    if len(da.reader.green_set) != 0:
        da.compute_green()
    if len(reader.fhv_set) != 0:
        da.compute_fhv()
    da.show_charts()
elif reader.type == Taxi_type.YELLOW:
    da.compute_yellow()
    da.yellow_chart.create_chart()
elif reader.type == Taxi_type.FHV:
    da.compute_fhv()
    da.fhv_chart.create_chart()
elif reader.type == Taxi_type.GREEN:
    da.compute_green()
    da.green_chart.create_chart()
