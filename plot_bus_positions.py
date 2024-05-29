#%%
import pandas
import geopandas
from shapely.geometry import LineString
from config import load_config
from connect import connect_to_db
import read


def main():
    connection = connect_to_db()
    data = read.vehicle_positions(connection)
    column_names = ("timestamp", "vehicle_id", "trip_id", "route_id", "longitude", "latitude")
    df = pandas.DataFrame(data, columns=column_names).sort_values(by=['timestamp'])
    
    plot_data(df)
    connection.close()
    

def plot_data(df):
    MINIMUM_NUMBER_OF_POINTS_TO_PLOT = int(load_config("plotting")["min_path_points"])

    vehicle_position_gdf = get_vehicle_position_gdf(df)
    vehicle_path_gdf = get_vehicle_path_gdf(vehicle_position_gdf,
                                            MINIMUM_NUMBER_OF_POINTS_TO_PLOT)
    
    base_shapefile = geopandas.read_file('assets/philadelphia.geojson')
    base_plot = base_shapefile.plot(color='white', edgecolor="black")
    vehicle_path_gdf.plot(ax=base_plot, column="trip_id", 
                          cmap="nipy_spectral", alpha=0.2)

    return base_plot


def get_vehicle_position_gdf(df):
    df["point"] = geopandas.points_from_xy(df.longitude, df.latitude)
    gdf = geopandas.GeoDataFrame(df, geometry="point", crs="EPSG:4326")

    return gdf


def get_vehicle_path_gdf(df, min_points:int = 0):
    df2 = pandas.DataFrame(df[["trip_id", "point"]])
    df2 = df2.groupby("trip_id")["point"].agg(list).reset_index(name="points")
    df2["point_count"] = df.groupby("trip_id")["trip_id"].size().reset_index(
        name="point_count")["point_count"]
    df2["path"] = df2["points"].apply(convert_points_to_linestring)

    if min_points:
        df2 = df2.query("point_count >= %i" %(min_points))

    gdf = geopandas.GeoDataFrame(df2, geometry="path", crs="EPSG:4326")
    return gdf


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