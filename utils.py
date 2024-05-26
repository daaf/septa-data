from datetime import datetime
from psycopg2 import sql

def write_to_db(connection, cursor, table_name, data: list[dict]):
    loaded_count = 0

    for entity in data:
        keys = list(entity.keys())
        values = list(entity.values())

        query = sql.SQL("INSERT INTO {table} ({keys}) VALUES ({value_placeholders});").format(
                table=sql.Identifier(table_name),
                keys=sql.SQL(', ').join(map(sql.Identifier, keys)),
                value_placeholders=sql.SQL(', ').join(sql.Placeholder() * len(keys)))

        try:
            cursor.execute(query, values)
            connection.commit()
            loaded_count += 1
        
        except Exception as e:
            print(e)

    write_to_console(f'Loaded data for {loaded_count} vehicles into the database')


def write_to_console(str):
    now = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    print(f'{now}   {str}')