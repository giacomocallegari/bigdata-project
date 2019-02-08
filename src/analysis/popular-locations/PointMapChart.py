#!/usr/bin/python
import shapefile
import csv
import matplotlib.colors
import matplotlib.pyplot as plt
from descartes import PolygonPatch
import os
from pyproj import Proj, transform
import pandas as pd
import config
class MapChart:

    dbf_path: str
    shp_path: str
    folder_path: str
    max_value: str
    min_value: str
    chart_name: str

    epsg_in = config.epsg_geo  # Initial EPSG
    epsg_out = config.epsg_ny  # Final EPSG

    def __init__(self, dbf_path = "", shp_path = "", folder_path= "", chart_name = "Taxi Chart"):
        self.dbf_path = dbf_path
        self.shp_path = shp_path
        self.folder_path = folder_path
        self.chart_name = chart_name

    def __get_shapefile_reader(self) -> shapefile.Reader:
        shp_file = open(self.shp_path, "rb")
        dbf_file = open(self.dbf_path, "rb")
        return shapefile.Reader(shp=shp_file, dbf=dbf_file)

    def __convert_projection(self, longitude, latitude):
        in_proj = Proj('+init=EPSG:' + self.epsg_in)  # Input projection
        out_proj = Proj('+init=EPSG:' + self.epsg_out, preserve_units=True)  # Output projection

        # print(longitude, latitude)
        x, y = transform(in_proj, out_proj, longitude, latitude)  # Convert the coordinates
        return x, y

    def add_points(self, folder_path, plt):
        file_list = os.listdir(folder_path)
        file_list.sort()
        for file_name in file_list:
            if file_name.startswith("part-") and file_name.endswith(".csv"):
                print("FILE " + file_name)
                for chunk in pd.read_csv(folder_path + "/" + file_name, chunksize=1000, header=None):
                    x, y = self.__convert_projection(chunk[1].values, chunk[0].values)
                    plt.plot(x, y, "y,")

    def create_chart(self):
        shapefile_reader = self.__get_shapefile_reader()
        figure = plt.figure(figsize=(10,8))
        axes = figure.gca()
        for shape in shapefile_reader.shapeRecords():
            poly_data = shape.shape.__geo_interface__
            axes.add_patch(PolygonPatch(poly_data, fc="black", ec="black", alpha=1, zorder=1))

        self.add_points(self.folder_path, plt)

        axes.axis('scaled')
        plt.axis("off")
        plt.show()

#
# a = MapChart(os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.dbf"), os.path.join(os.path.dirname(__file__), "../../shapefile/taxi_zones.shp"))
# a.create_chart()