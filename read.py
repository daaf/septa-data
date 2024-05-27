from psycopg2 import sql
from config import load_config

def vehicle_positions(connection):
    table_name = load_config(section="bus_trolley_positions")["table"]
    query = sql.SQL("""
            SELECT timestamp, vehicle_id, longitude, latitude, trip_id
            FROM {table}
            ORDER BY trip_id;
        """).format(table=sql.Identifier(table_name))
    
    return from_db(connection, query)


def from_db(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        
    return data