from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from fields_detection import init_fields
from typing import Dict
from TaxiType import TaxiType, type_names
from TimeScale import TimeScale, scale_names
from TimeChart import TimeChart
import DataReader
import Passengers
import os


class PassengersTime:
    spark = SparkSession \
        .builder \
        .appName("max-passengers-time") \
        .getOrCreate()

    taxi_type: TaxiType  # Type of taxi
    time_scale: TimeScale  # Time scale of the analysis
    chart: TimeChart  # Chart for the results
    multiple: bool  # Flag for analyzing multiple files
    fields: Dict[str, str]  # Fields of the dataset
    data_path: str  # Path of the output data


    # Initializes a new instance of the class.
    def __init__(self, reader, taxi_type, time_scale):
        self.reader = reader

        self.taxi_type = taxi_type
        self.time_scale = time_scale
        self.__set_multiple()
        self.__set_fields()
        self.__set_data_path()

    # Sets the multiple file flag.
    def __set_multiple(self):
        self.multiple = self.reader.is_a_folder

    # Sets the correct fields for the queries.
    def __set_fields(self):
        self.fields = init_fields(self.reader.type, self.reader.period)  # Initialize the correct fields for the queries

    # Sets the correct path for the output data.
    def __set_data_path(self):
        try:
            type_name = type_names[self.taxi_type.value]  # Set the name of the taxi type
            try:
                scale_name = scale_names[self.time_scale.value]  # Set the name of the time scale
                subpath = 'output-data/' + type_name + '-passengers-' + scale_name + 's'
                self.data_path = os.path.join(os.path.dirname(__file__), subpath)  # Set the output path
            except IndexError:
                print('Invalid time scale selected')
        except IndexError:
            print('Invalid taxi type selected')

    # Creates a DataFrame from the provided CSV file.
    def create_dataframe(self, csv_set: list) -> DataFrame:
        period = self.reader.period
        year = period[:4]
        fields = self.fields

        df = self.spark.read.format("csv").option("header", "true").load(csv_set).sample(fraction=1.0, withReplacement=False)
        if self.multiple: df = df.filter((df[fields['pu_time']].startswith(year)) & df[fields['do_time']].startswith(year))  # Ignore year outliers
        else: df = df.filter((df[fields['pu_time']].startswith(period) & df[fields['do_time']].startswith(period)))  # Ignore month outliers
        return df

    # Analyzes the provided data.
    def compute_data(self):
        print('Processing data...')

        tt = self.taxi_type
        ts = self.time_scale
        fields = self.fields

        if os.path.isdir(self.data_path):  # If the results CSV already exists, use it
            print('Reading an existing DataFrame...')

            passengers_time_df = self.spark.read.csv(self.data_path, inferSchema=True)
        else:  # Otherwise, perform the analysis
            print('Creating a new DataFrame...')

            # Read the taxi type
            if tt == TaxiType.YELLOW:
                df = self.create_dataframe(self.reader.yellow_set)
            elif tt == TaxiType.GREEN:
                df = self.create_dataframe(self.reader.green_set)
            elif tt == TaxiType.FHV:
                raise ValueError('FHV type not supported')
            else:
                raise ValueError('Invalid taxi type selected')

            # Read the time scale
            if ts == TimeScale.HOUR:
                passengers_time_df = Passengers.passengers_per_hour(df, fields)
            elif ts == TimeScale.DAY:
                passengers_time_df = Passengers.passengers_per_day(df, fields)
            elif ts == TimeScale.WEEKDAY:
                passengers_time_df = Passengers.passengers_per_weekday(df, fields)
            elif ts == TimeScale.MONTH:
                passengers_time_df = Passengers.passengers_per_month(df, fields)
            elif ts == TimeScale.YEAR:
                passengers_time_df = Passengers.passengers_per_year(df, fields)
            else:
                raise ValueError('Invalid time scale selected')

            passengers_time_df.write.csv(self.data_path, header=False)  # Save the results as a CSV file

        passengers_time_df.show()

        # Compute the value limits
        max_passenger = Passengers.max_passengers(passengers_time_df)
        min_time = Passengers.min_time(passengers_time_df)
        max_time = Passengers.max_time(passengers_time_df)

        # Initialize the correct labels
        type_name = type_names[tt.value].capitalize()
        scale_name = scale_names[ts.value].capitalize()

        self.chart = TimeChart(self.data_path, min_time, max_time, 0, max_passenger, ts, 'Passenger amount (USD)', type_name + ' Taxi Passengers - ' + scale_name + ' of departure')  # Create the chart

def analyze_passengers_time(time_scale):
    reader = DataReader.DataReader()  # Initialize the DataReader
    reader.read_input_params()  # Read the input parameters
    passengers_time = PassengersTime(reader, reader.type, time_scale)  # Initialize a new instance of PassengersTime

    passengers_time.compute_data()  # Compute the results
    passengers_time.chart.create_chart()  # Show the chart