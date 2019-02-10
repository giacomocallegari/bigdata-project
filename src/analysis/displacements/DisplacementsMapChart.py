#!/usr/bin/python
import shapefile
import csv
import matplotlib.colors
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from shapely import geometry
import os
import math


class DisplacementsMapChart:
    # Change this value to set the number of arrow visualized
    displacement_number = 100

    def __init__(self, dbf_path = "", shp_path = "", folder_income_path="", folder_outcome_path="", title=""):
        self.title = title
        self.dbf_path = dbf_path
        self.shp_path = shp_path
        self.folder_income_path = folder_income_path
        self.folder_outcome_path = folder_outcome_path

    def __get_shapefile_reader(self) -> shapefile.Reader:
        shp_file = open(self.shp_path, "rb")
        dbf_file = open(self.dbf_path, "rb")
        return shapefile.Reader(shp=shp_file, dbf=dbf_file)

    def __get_results_from_folder(self, folder):
        file_list = os.listdir(folder)
        file_list = sorted(file_list)
        results_list = []
        for file_name in file_list:
            if file_name.startswith("part-") and file_name.endswith(".csv"):
                with open(folder + "/" + file_name, newline='') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        departure, arrival, count = row
                        results_list.append(tuple([int(departure), int(arrival), int(count)]))
        return results_list

    def create_chart(self):
        shapefile_reader = self.__get_shapefile_reader()
        figure = plt.figure()
        axes_in = figure.add_subplot(121)
        axes_out = figure.add_subplot(122)
        centroid_list = dict()
        # Map Creation
        for shape in shapefile_reader.shapeRecords():
            # Draw neighborhood
            poly_data = shape.shape.__geo_interface__
            axes_in.add_patch(PolygonPatch(poly_data, fc="black", ec="white", alpha=0.6, zorder=1))
            axes_out.add_patch(PolygonPatch(poly_data, fc="black", ec="white", alpha=0.6,  zorder=1))
            # Draw centroid for each neighborhood
            neighborhood_poly = geometry.Polygon(shape.shape.points)
            centroid_x, centroid_y = neighborhood_poly.centroid.coords[0]
            centroid_list[shape.record[0]] = (centroid_x, centroid_y)
            axes_in.scatter([centroid_x],[centroid_y], s=3, color="black")
            axes_out.scatter([centroid_x], [centroid_y], s=3, color="black")

        incomes_records = self.__get_results_from_folder(self.folder_income_path)
        outcomes_records = self.__get_results_from_folder(self.folder_outcome_path)
        _, _, max_income_num = incomes_records[0]
        _, _, max_outcome_num = outcomes_records[0]

        # print(shapefile_reader.shapeRecords()[100].record[0:])
        if self.displacement_number != -1:
            incomes_records = incomes_records[0: self.displacement_number]
            outcomes_records = outcomes_records[0: self.displacement_number]
        self.displacement_number = len(incomes_records) + len(outcomes_records)
        print(max_income_num, " - ", max_outcome_num)
        norm = matplotlib.colors.Normalize(0, max(max_income_num, max_outcome_num))
        cmap = plt.cm.YlOrRd
        index = 0
        for outcome_record in reversed(outcomes_records):
            _, percentage = math.modf((index / self.displacement_number) * 100)
            percentage = int(percentage)
            print("\rBuilding map - chart -> " + str(percentage) + "%", end="")
            index += 1
            out_departure, out_arrival, out_count = outcome_record
            center_out_from_x, center_out_from_y = centroid_list.get(out_departure)
            center_out_to_x, center_out_to_y = centroid_list.get(out_arrival)
            axes_out.arrow(center_out_from_x,
                           center_out_from_y,
                           center_out_to_x - center_out_from_x,
                           center_out_to_y - center_out_from_y,
                           color=str(matplotlib.colors.rgb2hex(cmap(norm(out_count)))),
                           width=100,
                           alpha=1,
                           length_includes_head=True)

        for income_record in reversed(incomes_records):
            _, percentage = math.modf((index/self.displacement_number) * 100)
            percentage = int(percentage)
            print("\rBuilding map - chart -> " + str(percentage) + "%", end="")
            index += 1
            in_departure, in_arrival, in_count = income_record
            center_in_from_x, center_in_from_y = centroid_list.get(in_departure)
            center_in_to_x, center_in_to_y = centroid_list.get(in_arrival)
            axes_in.arrow(center_in_from_x,
                      center_in_from_y,
                      center_in_to_x - center_in_from_x,
                      center_in_to_y - center_in_from_y,
                      color=str(matplotlib.colors.rgb2hex(cmap(norm(in_count)))),
                      width=100,
                      alpha=1,
                      length_includes_head=True)
        print("\rBuilding map - chart -> 100%", end="")
        # sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        # sm.set_array([])
        # figure.colorbar(sm)
        axes_in.axis('scaled')
        axes_out.axis('scaled')
        axes_in.axis("off")
        axes_out.axis("off")
        axes_in.set_title("Incomes")
        axes_out.set_title("Outcomes")
        figure.suptitle(self.title + " (only the first " + str(self.displacement_number*2) + ")")
        # Put all in fullscreen
        mng = plt.get_current_fig_manager()
        mng.full_screen_toggle()
        plt.show()

