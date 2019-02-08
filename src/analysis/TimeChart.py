#!/usr/bin/python
import os
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
from TimeScale import TimeScale


class TimeChart:

    folder_path: str  # Path of the output folder
    min_time: int  # Minimum time on X-axis
    max_time: int  # Maximum time on X-axis
    min_value: float  # Minimum value on Y-axis
    max_value: float  # Maximum value on Y-axis
    time_scale: TimeScale  # Time scale (hour, day or month)
    data_label: str  # Label of the Y-axis
    chart_name: str  # Title of the chart

    # Sets the correct parameters for the chart.
    def __init__(self, folder_path='', min_time = 0, max_time = 0, min_value = 0, max_value = 0, time_scale = TimeScale.NONE, data_label ='Time', chart_name ="Taxi Chart"):
        self.folder_path = folder_path
        self.min_time = min_time
        self.max_time = max_time
        self.min_value = min_value
        self.max_value = max_value
        self.time_scale = time_scale
        self.data_label = data_label
        self.chart_name = chart_name

    # Gets the data to plot on the chart.
    def __get_results(self):
        file_list = os.listdir(self.folder_path)
        dict_list = dict()
        for file_name in file_list:
            if file_name.startswith("part-") and file_name.endswith(".csv"):
                with open(self.folder_path + "/" + file_name, newline='') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        key, value = row
                        dict_list[int(key)] = float(value)
        return dict_list

    # Creates the chart.
    def create_chart(self):
        ts = self.time_scale # Get the time scale (hour, day or month)

        # Scale-dependent parameters
        xmin = self.min_time
        xmax = self.max_time
        x_major_step = 0
        xlabel = ''

        if ts == TimeScale.HOUR:  # Hour of the day
            x_major_step = 1
            xlabel = 'Time of the day (h)'
        elif ts == TimeScale.DAY:  # Day of the year
            x_major_step = 7
            xlabel = 'Day of the year (D)'
        elif ts == TimeScale.MONTH:  # Month of the year
            x_major_step = 3
            xlabel = 'Month of the year (M)'
        elif ts == TimeScale.YEAR:  # Year
            x_major_step = 1
            xlabel = 'Year (Y)'
        else:
            print('Invalid time scale')
            return

        # Set the major and minor ticks on the axes
        x_major_locator = MultipleLocator(x_major_step)  # X major ticks
        x_minor_locator = MultipleLocator(1)  # X minor ticks
        y_major_locator = MultipleLocator(1)  # Y major ticks
        y_minor_locator = MultipleLocator(0.25)  # Y minor ticks

        fig, ax = plt.subplots()
        ax.xaxis.set_major_locator(x_major_locator)
        ax.xaxis.set_minor_locator(x_minor_locator)
        ax.yaxis.set_major_locator(y_major_locator)
        ax.yaxis.set_minor_locator(y_minor_locator)

        # Sort the data and get the lists by axis
        data = sorted(self.__get_results().items())
        x, y = zip(*data)

        # Compute the minimum and maximum Y values
        ymin = self.min_value
        ymax = self.max_value * 1.25

        # Range limits
        plt.xlim(xmin, xmax)
        plt.ylim(ymin, ymax)

        # Axis labels and chart title
        plt.xlabel(xlabel)
        plt.ylabel(self.data_label)
        plt.title(self.chart_name)

        # Create and show the chart
        plt.scatter(x, y)  # Add the points
        plt.plot(x, y)  # Draw the line
        plt.grid(axis='x', which='major')  # Add the major X grid
        plt.grid(axis='y', which='major')  # Add the major Y grid
        plt.grid(axis='x', which='minor', linestyle='--')  # Add the minor X grid
        plt.grid(axis='y', which='minor', linestyle='--')  # Add the minor Y grid
        plt.show()  # Show the chart