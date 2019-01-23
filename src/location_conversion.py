import shapefile
from shapely.geometry import Point
from shapely.geometry import shape
from pyproj import Proj, transform
import config


path = config.shp_path  # Path of the shapefile
filename = config.shp_name  # Name of the shapefile
epsg_in = config.epsg_geo  # Initial EPSG
epsg_out = config.epsg_ny  # Final EPSG


# Converts coordinates from a projection to another.
def __convert_projection(longitude, latitude):
    in_proj = Proj('+init=EPSG:' + epsg_in)  # Input projection
    out_proj = Proj('+init=EPSG:' + epsg_out, preserve_units=True)  # Output projection

    # print(longitude, latitude)
    x, y = transform(in_proj, out_proj, longitude, latitude)  # Convert the coordinates
    return x, y


# Converts a pair of coordinates to the relative zone.
def coordinates_to_zone(longitude, latitude):
    point = __convert_projection(longitude, latitude)  # Create a new point by converting the coordinates
    shp = shapefile.Reader(path + filename)  # Read the shapefile
    all_shapes = shp.shapes()  # Get the list of shapes
    all_records = shp.records()  # Get the list of records

    for i in range(len(all_shapes)):  # Iterate through the shapes
        boundary = all_shapes[i]

        if Point(point).within(shape(boundary)):  # Check if the point is within the current shape
            zone_id = all_records[i][4]  # Find the id of the shape
            return zone_id


# coordinates_to_zone(-74.007818, 40.740279999999998)  # Test
