from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from fields_detection import init_fields
import TaxiType
import DataReader
import Passengers
import PassengersMapChart as MapChart
import os
import shutil


class PassengersDepartures:
    spark = SparkSession \
        .builder \
        .appName("max-passengers-departures") \
        .getOrCreate()

    yellow_data_path = os.path.join(os.path.dirname(__file__), "output-data/yellow-passengers-departures")
    green_data_path = os.path.join(os.path.dirname(__file__), "output-data/green-passengers-departures")
    shp_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.shp")
    dbf_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.dbf")

    yellow_chart = MapChart.MapChart()
    green_chart = MapChart.MapChart()
    fhv_chart = MapChart.MapChart()

    def __init__(self, reader):
        self.reader = reader

    def create_dataframe(self, csv_set: list) -> DataFrame:
        return self.spark.read.format("csv").option("header", "true").load(csv_set)

    # Creates a chart to visualize the results of the analysis.
    def show_charts(self):
        # Creation of chart concurrently
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

    # Analyzes data of type yellow.
    def compute_yellow(self):
        print("Processing yellow taxi,..")
        yellow_df = self.create_dataframe(self.reader.yellow_set)
        if fields['pu_loc'] == '' and fields['do_loc'] == '':
            yellow_df = Passengers.convert_df(yellow_df, fields)
        passengers_pu_area_df = Passengers.passengers_per_pickup_area(yellow_df, fields)
        max_passengers = Passengers.max_passengers(passengers_pu_area_df)
        if os.path.isdir(self.yellow_data_path):
            shutil.rmtree(self.yellow_data_path)
        passengers_pu_area_df.write.csv(self.yellow_data_path, header=False)
        self.yellow_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.yellow_data_path, 0, max_passengers, "Yellow Taxi Passengers - Departure Zone")

    # Analyzes data of type green.
    def compute_green(self):
        print("Processing green taxi,..")
        green_df = self.create_dataframe(self.reader.green_set)
        if fields['pu_loc'] == '' and fields['do_loc'] == '':
            green_df = Passengers.convert_df(green_df, fields)
        passengers_pu_area_df = Passengers.passengers_per_pickup_area(green_df, fields)
        max_passengers = Passengers.max_passengers(passengers_pu_area_df)
        if os.path.isdir(self.green_data_path):
            shutil.rmtree(self.green_data_path)
        passengers_pu_area_df.write.csv(self.green_data_path, header=False)
        self.green_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.green_data_path, 0, max_passengers, "Green Taxi Passengers - Departure Zone")


reader = DataReader.DataReader()  # Initialize the DataReader
reader.read_input_params()  # Read the input parameters
aa = PassengersDepartures(reader)  # Set the reader
Taxi_type = TaxiType.TaxiType  # Set the file type

# Initialize the correct fields for the queries
fields = init_fields(aa.reader.type, aa.reader.period)

# Perform the analysis for the desired types
if aa.reader.type == Taxi_type.ALL:
    if len(aa.reader.yellow_set) != 0:
        aa.compute_yellow()
    if len(aa.reader.green_set) != 0:
        aa.compute_green()
    if len(reader.fhv_set) != 0:
        print("FHV type not supported")
    aa.show_charts()
elif reader.type == Taxi_type.YELLOW:
    aa.compute_yellow()
    aa.yellow_chart.create_chart()
elif reader.type == Taxi_type.GREEN:
    aa.compute_green()
    aa.green_chart.create_chart()
elif reader.type == Taxi_type.FHV:
    print("FHV type not supported")