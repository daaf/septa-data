from psycopg2 import sql
from config import load_config

def vehicle_positions(connection):
    table_name = load_config(section="tables")["bus_trolley_positions"]
    query = sql.SQL("""
            SELECT timestamp, vehicle_id, trip_id, route_id, longitude, latitude
            FROM {table}
            ORDER BY timestamp;
        """).format(table=sql.Identifier(table_name))
    
    return from_db(connection, query)


def from_db(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        
    return data