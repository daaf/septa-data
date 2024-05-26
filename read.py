from psycopg2 import sql


def from_db(connection, table_name: str, column_names: tuple[str]):
    query = sql.SQL("SELECT {columns} FROM {table};").format(
        columns=sql.SQL(', ').join(map(sql.Identifier, column_names)),
        table=sql.Identifier(table_name))

    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        
    return data