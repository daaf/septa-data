from psycopg2 import sql


def vehicle_positions(connection, table_name: str):
    query = sql.SQL("""
            SELECT timestamp, vehicle_id, longitude, latitude, trip_id
            FROM {table}
            ORDER BY trip_id;
        """).format(table=sql.Identifier(table_name))
    
    return from_db(connection, query)


def vehicle_positions_by_trip(connection, table_name:str):
    query = sql.SQL("""
            SELECT trip_id, vehicle_id, COUNT(*) AS trip_count, array_agg()
            FROM {table}
            GROUP BY trip_id, vehicle_id
            ORDER BY trip_count DESC;
        """).format(table=sql.Identifier(table_name))
    
    return from_db(connection, query)


def from_db(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        
    return data