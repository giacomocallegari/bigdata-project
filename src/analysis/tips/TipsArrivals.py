from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from fields_detection import init_fields
from location_conversion import coordinates_to_zone
import TaxiType
import DataReader
import TipsMapChart as MapChart
import os
import shutil


class Tips:
    spark = SparkSession \
        .builder \
        .appName("max-tips-arrivals") \
        .getOrCreate()

    yellow_data_path = os.path.join(os.path.dirname(__file__), "output-data/yellow-tips")
    green_data_path = os.path.join(os.path.dirname(__file__), "output-data/green-tips")
    fhv_data_path = os.path.join(os.path.dirname(__file__), "output-data/fhv-tips")
    shp_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.shp")
    dbf_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.dbf")

    yellow_chart = MapChart.MapChart()
    green_chart = MapChart.MapChart()
    fhv_chart = MapChart.MapChart()

    def __init__(self, reader):
        self.reader = reader

    def create_dataframe(self, csv_set: list) -> DataFrame:
        return self.spark.read.format("csv").option("header", "true").load(csv_set)

    @staticmethod
    # Finds the maximum tip in a DataFrame.
    def __max_tips(tips_df: DataFrame):
        max_tip = tips_df.collect()[0]
        return max_tip['avg(' + tip + ')']

    @staticmethod
    # Converts coordinates to zones.
    def __convert_df(taxi_df: DataFrame) -> DataFrame:
        zone_udf = udf(lambda lon, lat: coordinates_to_zone(lon, lat))

        taxi_df = taxi_df.withColumn('pu_loc', zone_udf(taxi_df[pu_lon], taxi_df[pu_lat]))  # Convert pick-up coordinates
        taxi_df = taxi_df.withColumn('do_loc', zone_udf(taxi_df[do_lon], taxi_df[do_lat]))  # Convert drop-off coordinates

        return taxi_df

    @staticmethod
    # Finds the tip amount per drop-off location.
    def tips_per_dropoff_area(taxi_df: DataFrame) -> DataFrame:
        return taxi_df.groupBy(do_loc).agg(avg(tip)).orderBy('avg(' + tip + ')', ascending=False)

    '''
    @staticmethod
    # Finds the tip amount per hour of the day.
    def tips_per_hour(taxi_df: DataFrame) -> DataFrame:
        return taxi_df.groupBy(hour(do_time).alias('hour_of_day')).agg(avg(tip)).orderBy('avg(' + tip + ')', ascending=False)

    @staticmethod
    # Finds the tip amount per day of the year.
    def tips_per_day(taxi_df: DataFrame) -> DataFrame:
        return taxi_df.groupBy(dayofyear(do_time).alias('day_of_year')).agg(avg(tip)).orderBy('avg(' + tip + ')', ascending=False)

    @staticmethod
    # Finds the tip amount per month of the year.
    def tips_per_month(taxi_df: DataFrame) -> DataFrame:
        taxi_df.groupBy(month(do_time).alias('month_of_year')).agg(avg(tip)).orderBy('avg(' + tip + ')', ascending=False)
    '''

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
        if pu_loc == '' and do_loc == '':
            yellow_df = self.__convert_df(yellow_df)
        tips_do_area_df = self.tips_per_dropoff_area(yellow_df)
        max_tip = self.__max_tips(tips_do_area_df)
        if os.path.isdir(self.yellow_data_path):
            shutil.rmtree(self.yellow_data_path)
        tips_do_area_df.write.csv(self.yellow_data_path, header=False)
        self.yellow_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.yellow_data_path, 0, max_tip, "Yellow Taxi Tips - Arrival Zone")

    # Analyzes data of type green.
    def compute_green(self):
        print("Processing green taxi,..")
        green_df = self.create_dataframe(self.reader.green_set)
        if pu_loc == '' and do_loc == '':
            green_df = self.__convert_df(green_df)
        tips_do_area_df = self.tips_per_dropoff_area(green_df)
        max_tip = self.__max_tips(tips_do_area_df)
        if os.path.isdir(self.green_data_path):
            shutil.rmtree(self.green_data_path)
        tips_do_area_df.write.csv(self.green_data_path, header=False)
        self.green_chart = MapChart.MapChart(self.dbf_file, self.shp_file, self.green_data_path, 0, max_tip, "Green Taxi Tips - Arrival Zone")


reader = DataReader.DataReader()  # Initialize the DataReader
reader.read_input_params()  # Read the input parameters
aa = Tips(reader)  # Set the reader
Taxi_type = TaxiType.TaxiType  # Set the file type

# Initialize the correct fields for the queries
fields = init_fields(aa.reader.type, '2018-04')  # DEBUG
pu_loc = fields['pu_loc']
do_loc = fields['do_loc']
pu_lon = fields['pu_lon']
pu_lat = fields['pu_lat']
do_lon = fields['do_lon']
do_lat = fields['do_lat']
tip = fields['tip']

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
elif reader.type == Taxi_type.FHV:
    print("FHV type not supported")
elif reader.type == Taxi_type.GREEN:
    aa.compute_green()
    aa.green_chart.create_chart()