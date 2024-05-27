#%%
import numpy
import pandas
import geopandas
from shapely.geometry import LineString
from typing import Iterable
from connect import connect_to_db
import read


def main():
    connection = connect_to_db()
    data = read.vehicle_positions(connection)
    column_names = ("timestamp", "vehicle_id", "longitude", "latitude", "trip_id")
    df = pandas.DataFrame(data, columns=column_names).sort_values(by=['timestamp'])
    
    plot(df)
    connection.close()
    

def plot(df):
    vehicle_position_gdf = get_vehicle_position_gdf(df)
    vehicle_path_gdf = get_vehicle_path_gdf(vehicle_position_gdf)

    base_shapefile = geopandas.read_file('assets/philadelphia.geojson')
    base_plot = base_shapefile.plot(color='white', edgecolor="black")
    # vehicle_position_gdf.plot(ax=base_plot, markersize=5, column="trip_id")
    vehicle_path_gdf.plot(ax=base_plot, markersize=5, column="trip_id")

    return base_plot


def get_vehicle_position_gdf(df):
    df["point"] = geopandas.points_from_xy(df.longitude, df.latitude)
    gdf = geopandas.GeoDataFrame(df, geometry="point", crs="EPSG:4326")

    return gdf


def get_vehicle_path_gdf(df):
    df2 = pandas.DataFrame(df[["trip_id", "point"]])
    df2 = df2.groupby("trip_id")["point"].agg(list).reset_index(name="points")
    df2["point_count"] = df.groupby("trip_id")["trip_id"].size().reset_index(name="point_count")["point_count"]
    df2["path"] = df2["points"].apply(convert_points_to_linestring)
    gdf = geopandas.GeoDataFrame(df2, geometry="path", crs="EPSG:4326")

    return gdf.query("point_count > 25")


def convert_points_to_linestring(points: list):
    filtered_points = list(filter(lambda point: not point.is_empty, points))
    output = None
    
    if len(filtered_points) > 1:
        try:
            output = LineString(filtered_points)
        except Exception as error:
            print(error)
            print(f'Point conversion failed for points: {points}')
    
    return output


if __name__ == "__main__":
    main()