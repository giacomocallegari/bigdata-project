from matplotlib import colors
import numpy as np
import shapefile
import math
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from shapely.geometry import shape as shp
from shapely.geometry import Point
from pyproj import Proj, transform
from pyspark.sql.functions import *
import config
from pyspark.sql.types import BooleanType
from pyspark.sql import SparkSession
from timeit import default_timer as timer
import csv
import os
import hashlib
# data = np.random.rand(10, 10) * 20
# print(data)
# # create discrete colormap
# cmap = colors.ListedColormap(['red', 'blue'])
# bounds = [0,10,20]
# norm = colors.BoundaryNorm(bounds, 2)
# print(norm)
#
# fig, ax = plt.subplots()
# ax.imshow(data, cmap=cmap, norm=norm)
#
# # draw gridlines
# ax.grid(which='major', axis='both', linestyle='-', color='k', linewidth=2)
# ax.set_xticks(np.arange(-.5, 10, 1));
# ax.set_yticks(np.arange(-.5, 10, 1));
#
# plt.show()


class MatrixMap:
    def __init__(self, cells_dim, shp_path, dbf_path):
        self.cells_dim = cells_dim
        self.shp_path = shp_path
        self.dbf_path = dbf_path
        # shp_file = open(self.shp_path, "rb")
        # dbf_file = open(self.dbf_path, "rb")
        # self.shapefile_reader = shapefile.Reader(shp=shp_file, dbf=dbf_file)
        (self.minx, self.maxx), (self.miny, self.maxy) = self.get_min_max_lat_long()
        self.matrix = self.get_matrix()

    def get_min_max_lat_long(self):
        min_long = math.inf
        max_long = - math.inf
        min_lat = math.inf
        max_lat = - math.inf
        shp_file = open(self.shp_path, "rb")
        dbf_file = open(self.dbf_path, "rb")
        for shape in shapefile.Reader(shp=shp_file, dbf=dbf_file).shapeRecords():
            poly_data = shape.shape.__geo_interface__
            current_min_long, current_min_lat, current_max_long, current_max_lat = shp(poly_data).bounds
            if current_min_lat < min_lat: min_lat = current_min_lat
            if current_max_lat > max_lat: max_lat = current_max_lat
            if current_min_long < min_long: min_long = current_min_long
            if current_max_long > max_long: max_long = current_max_long

        return (min_long, max_long), (min_lat, max_lat)

    def get_coord_by_index(self, col_index, row_index):
        lon =  self.minx + (self.cells_dim * (col_index - 1)) + (self.cells_dim / 2)
        lat = self.miny + (self.cells_dim * (row_index - 1)) + (self.cells_dim / 2)
        return lon, lat

    def get_index_by_coord(self, lon, lat):
        col_index = int((lon - self.minx) / self.cells_dim)
        row_index = int((lat - self.miny) / self.cells_dim)
        return col_index, row_index

    def get_neighborhood_id(self, lon, lat):
        # print(lon, " - ", lat)
        if not(self.minx < lon < self.maxx) or not(self.miny < lat < self.maxy):
            return 0
        else:
            # starta = timer()
            col_index, row_index = self.get_index_by_coord(lon, lat)
            # enda = timer()
            # startb = timer()
            a = self.matrix[row_index][col_index]
            # endb = timer()
            # print(enda - starta, "- ", endb - startb)
            return a

    def get_matrix(self):
        code = self.shp_path + self.dbf_path + str(self.cells_dim)
        h = hashlib.md5()
        h.update(code.encode('utf-8'))
        hash_code = h.hexdigest()
        matrix = None
        if os.path.exists(hash_code+".csv"):
            matrix = np.genfromtxt(str(hash_code)+".csv", delimiter=",")
        else:
            col_num = int((self.maxx - self.minx) / self.cells_dim) + 1
            row_num = int((self.maxy - self.miny) / self.cells_dim) + 1
            # matrix = [[0]*col_num] * row_num
            matrix = np.full((row_num, col_num), 0)
            shape_id = 0
            shp_file = open(self.shp_path, "rb")
            dbf_file = open(self.dbf_path, "rb")
            for shape in shapefile.Reader(shp=shp_file, dbf=dbf_file).shapeRecords():
                shape_id += 1
                poly_data = shape.shape.__geo_interface__
                current_min_long, current_min_lat, current_max_long, current_max_lat = shp(poly_data).bounds
                min_col_index, min_row_index = self.get_index_by_coord(current_min_long, current_min_lat)
                max_col_index, max_row_index = self.get_index_by_coord(current_max_long, current_max_lat)
                print(shape_id)
                for i in range(min_row_index, max_row_index + 1):
                    for j in range(min_col_index, max_col_index + 1):
                        lon_point, lat_point = self.get_coord_by_index(j,i)
                        point = Point(lon_point, lat_point)
                        if point.within(shp(shape.shape.__geo_interface__)):
                            matrix[i][j] = shape_id
            with open(hash_code+ ".csv", mode="w") as matrix_file:
                matrix_wr = csv.writer(matrix_file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
                for row in matrix:
                    matrix_wr.writerow(row)
            matrix_file.close()

        matrix = matrix.tolist()
        return matrix

# a = MatrixMap(2000, "shapefile/taxi_zones.shp", "shapefile/taxi_zones.dbf")
#
# matrix = a.get_matrix()
# matrix[0][0] = 10
# plt.matshow(matrix)
# plt.gca().invert_yaxis()
# plt.show()
