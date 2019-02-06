from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from fields_detection import init_fields
from TaxiType import TaxiType, type_names
from TimeScale import TimeScale, scale_names
from TipsTimeChart import TimeChart
import DataReader
import Tips
import os
import shutil


class TipsTime:
    spark = SparkSession \
        .builder \
        .appName("max-tips-time") \
        .getOrCreate()

    taxi_type: TaxiType  # Type of taxi
    time_scale: TimeScale  # Time scale of the analysis
    chart: TimeChart  # Chart for the results
    data_path: str  # Path of the output data


    # Initializes a new instance of the class.
    def __init__(self, reader, taxi_type, time_scale):
        self.reader = reader

        self.taxi_type = taxi_type
        self.time_scale = time_scale
        self.set_data_path()

    # Sets the correct path for the output data.
    def set_data_path(self):
        try:
            type_name = type_names[self.taxi_type.value]  # Set the name of the taxi type
            try:
                scale_name = scale_names[self.time_scale.value]  # Set the name of the time scale
                subpath = 'output-data/' + type_name + '-tips-' + scale_name + 's'
                self.data_path = os.path.join(os.path.dirname(__file__), subpath)  # Set the output path
            except IndexError:
                print('Invalid time scale selected')
        except IndexError:
            print('Invalid taxi type selected')

    def create_dataframe(self, csv_set: list) -> DataFrame:
        return self.spark.read.format("csv").option("header", "true").load(csv_set)

    # Analyzes the provided data.
    def compute_data(self, fields):
        print('Processing data...')

        tt = self.taxi_type
        ts = self.time_scale

        df = DataFrame
        tips_time_df = DataFrame

        # Read the taxi type
        if tt == TaxiType.YELLOW: df = self.create_dataframe(self.reader.yellow_set)
        elif tt == TaxiType.GREEN: df = self.create_dataframe(self.reader.green_set)
        elif tt == TaxiType.FHV: print('FHV type not supported')
        else: raise ValueError('Invalid taxi type selected')

        # Read the time scale
        if ts == TimeScale.HOUR: tips_time_df = Tips.tips_per_hour(df, fields)
        elif ts == TimeScale.DAY: tips_time_df = Tips.tips_per_day(df, fields)
        elif ts == TimeScale.MONTH: tips_time_df = Tips.tips_per_month(df, fields)
        elif ts == TimeScale.YEAR: tips_time_df = Tips.tips_per_year(df, fields)
        else: raise ValueError('Invalid time scale selected')

        max_tip = Tips.max_tip(tips_time_df)
        min_time = Tips.min_time(tips_time_df)
        max_time = Tips.max_time(tips_time_df)

        if os.path.isdir(self.data_path):
            shutil.rmtree(self.data_path)
        tips_time_df.write.csv(self.data_path, header=False)

        type_name = type_names[tt.value].capitalize()
        scale_name = scale_names[ts.value].capitalize()
        self.chart = TimeChart(self.data_path, min_time, max_time, 0, max_tip, ts, 'Tip amount (USD)', type_name + ' Taxi Tips - ' + scale_name + ' of departure')

def analyze_tips_time(time_scale):
    reader = DataReader.DataReader()  # Initialize the DataReader
    reader.read_input_params()  # Read the input parameters
    tips_time = TipsTime(reader, reader.type, time_scale)  # Initialize a new instance of TipsTime

    fields = init_fields(tips_time.reader.type, tips_time.reader.period)  # Initialize the correct fields for the queries
    tips_time.compute_data(fields)  # Compute the results
    tips_time.chart.create_chart()  # Show the chart