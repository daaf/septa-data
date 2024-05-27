#%%
import numpy
import pandas
import geopandas
from typing import Iterable
from config import load_config
from connect import connect_to_db
import read


def main():
    connection = connect_to_db()
    table_name = load_config(section="bus_trolley_positions")["table"]
    column_names = ("timestamp", "vehicle_id", "longitude", "latitude", "trip_id")
    gdf = get_geopandas_dataframe(connection, table_name, column_names)
    
    plot(gdf)
    connection.close()


def get_geopandas_dataframe(connection, table_name, column_names):
    data = read.from_db(connection, table_name, column_names)
    df = pandas.DataFrame(data, columns=column_names)
    marker_colors = get_random_color_per_unique_value(df["trip_id"], alpha=0.3)
    df["color"] = df["trip_id"].map(lambda id: marker_colors[id])
    gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")

    return gdf


def plot(gdf):
    base_shapefile = geopandas.read_file('assets/philadelphia.geojson')
    base = base_shapefile.plot(color='white', edgecolor="black")
    gdf.plot(ax=base, markersize=5, c=gdf["color"])


def get_random_color_per_unique_value(values:Iterable, alpha=None):
    colors = {}

    for item in values:
        if item not in colors:
            new_color = list(numpy.random.uniform(0,1, size=3))
            
            if alpha:
                new_color.append(alpha)
            
            colors[item] = new_color

    return colors


if __name__ == "__main__":
    main()