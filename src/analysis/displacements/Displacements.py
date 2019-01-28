from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import TaxiType
import DataReader
import shutil
from DisplacementsMapChart import DisplacementsMapChart as chart
import os


class Displacement:
    spark = SparkSession \
        .builder \
        .appName("max-trips") \
        .getOrCreate()

    yellow_data_income_path = os.path.join(os.path.dirname(__file__), "output-data/yellow-income-displacements")
    yellow_data_outcome_path = os.path.join(os.path.dirname(__file__), "output-data/yellow-outcome-displacements")
    green_data_income_path = os.path.join(os.path.dirname(__file__), "output-data/green-income-displacements")
    green_data_outcome_path = os.path.join(os.path.dirname(__file__), "output-data/green-outcome-displacements")
    fhv_data_income_path = os.path.join(os.path.dirname(__file__), "output-data/fhv-income-displacements")
    fhv_data_outcome_path = os.path.join(os.path.dirname(__file__), "output-data/fhv-outcome-displacements")
    green_data_path = os.path.join(os.path.dirname(__file__), "output-data/green-displacements")
    fhv_data_path = os.path.join(os.path.dirname(__file__), "output-data/fhv-displacements")
    shp_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.shp")
    dbf_file = os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.dbf")

    yellow_chart = chart()
    green_chart = chart()
    fhv_chart = chart()

    def __init__(self, reader):
        self.reader = reader

    def create_dataframe(self, csv_set: list) -> DataFrame:
        return self.spark.read.format("csv").option("header", "true").load(csv_set)

    def compute_yellow(self):
        print("Processing yellow taxi,..")
        yellow_df = self.create_dataframe(self.reader.yellow_set)
        income, outcome = self.count_displacements(yellow_df)
        if os.path.isdir(self.yellow_data_income_path):
            shutil.rmtree(self.yellow_data_income_path)
        if os.path.isdir(self.yellow_data_outcome_path):
            shutil.rmtree(self.yellow_data_outcome_path)
        income.write.csv(self.yellow_data_income_path, header=False)
        outcome.write.csv(self.yellow_data_outcome_path, header=False)
        self.yellow_chart = chart(self.dbf_file, self.shp_file, self.yellow_data_income_path, self.yellow_data_outcome_path, "Yellow Displacements")

    def compute_green(self):
        print("Processing green taxi,..")
        green_df = self.create_dataframe(self.reader.green_set)
        income, outcome = self.count_displacements(green_df)
        if os.path.isdir(self.green_data_income_path):
            shutil.rmtree(self.green_data_income_path)
        if os.path.isdir(self.green_data_outcome_path):
            shutil.rmtree(self.green_data_outcome_path)
        income.write.csv(self.green_data_income_path, header=False)
        outcome.write.csv(self.green_data_outcome_path, header=False)
        self.green_chart = chart(self.dbf_file, self.shp_file, self.green_data_income_path, self.green_data_outcome_path, "Green Displacements")

    def compute_fhv(self):
        print("Processing fhv taxi,..")
        fhv_df = self.create_dataframe(self.reader.fhv_set)
        income, outcome = self.count_displacements(fhv_df)
        if os.path.isdir(self.fhv_data_income_path):
            shutil.rmtree(self.fhv_data_income_path)
        if os.path.isdir(self.fhv_data_outcome_path):
            shutil.rmtree(self.fhv_data_outcome_path)
        income.write.csv(self.fhv_data_income_path, header=False)
        outcome.write.csv(self.fhv_data_outcome_path, header=False)
        self.fhv_chart = chart(self.dbf_file, self.shp_file, self.fhv_data_income_path, self.fhv_data_outcome_path, "FHV Displacements")

    def count_displacements(self, taxi_df: DataFrame):
        displacements = taxi_df.filter((col("PULocationID") != "") &
                       (col("PULocationID") < 264) &
                       (col("DOLocationID") != "") &
                       (col("DOLocationID") < 264) &
                       (col("PULocationID") != col("DOLocationID"))).groupBy("PULocationID", "DOLocationID").count().orderBy(desc("count"))
        return displacements.filter(col("PULocationID") < col("DOLocationID")), displacements.filter(col("PULocationID") > col("DOLocationID"))

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


reader = DataReader.DataReader()
reader.read_input_params()
total_dis = Displacement(reader)
Taxi_type = TaxiType.TaxiType

if total_dis.reader.type == Taxi_type.ALL:
    if len(total_dis.reader.yellow_set) != 0:
        total_dis.compute_yellow()
    if len(total_dis.reader.green_set) != 0:
        total_dis.compute_green()
    if len(reader.fhv_set) != 0:
        total_dis.compute_fhv()
    total_dis.show_charts()
elif reader.type == Taxi_type.YELLOW:
    total_dis.compute_yellow()
    total_dis.yellow_chart.create_chart()
elif reader.type == Taxi_type.FHV:
    total_dis.compute_fhv()
    total_dis.fhv_chart.create_chart()
elif reader.type == Taxi_type.GREEN:
    total_dis.compute_green()
    total_dis.green_chart.create_chart()
