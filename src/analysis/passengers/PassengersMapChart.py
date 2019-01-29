#!/usr/bin/python
import shapefile
import csv
import matplotlib.colors
import matplotlib.pyplot as plt
from descartes import PolygonPatch
import os


class MapChart:

    dbf_path: str
    shp_path: str
    folder_path: str
    max_value: str
    min_value: str
    chart_name: str

    def __init__(self, dbf_path = "", shp_path = "", folder_path= "", min_value = 0,  max_value = 0, chart_name = "Taxi Chart"):
        self.dbf_path = dbf_path
        self.shp_path = shp_path
        self.folder_path = folder_path
        self.max_value = max_value
        self.min_value = min_value
        self.chart_name = chart_name

    def __get_shapefile_reader(self) -> shapefile.Reader:
        shp_file = open(self.shp_path, "rb")
        dbf_file = open(self.dbf_path, "rb")
        return shapefile.Reader(shp=shp_file, dbf=dbf_file)

    def __get_results(self):
        file_list = os.listdir(self.folder_path)
        dict_list = dict()
        for file_name in file_list:
            if file_name.startswith("part-") and file_name.endswith(".csv"):
                with open(self.folder_path + "/" + file_name, newline='') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        key, value = row
                        dict_list[int(key)] = int(float(value))
        return dict_list

    def create_chart(self):
        shapefile_reader = self.__get_shapefile_reader()
        results_list = self.__get_results()
        figure = plt.figure(figsize=(10,8))
        cmap = plt.cm.Oranges
        norm = matplotlib.colors.Normalize(self.min_value, self.max_value)
        axes = figure.gca()
        counter = 1
        for shape in shapefile_reader.shapeRecords():
            poly_data = shape.shape.__geo_interface__
            axes.add_patch(PolygonPatch(poly_data, fc=str(matplotlib.colors.rgb2hex(cmap(norm(results_list.get(counter, self.min_value))))), ec="black", alpha=1, zorder=1))
            counter = counter + 1
        axes.axis('scaled')
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        figure.colorbar(sm)
        plt.axis("off")
        plt.title(self.chart_name)
        plt.show()
