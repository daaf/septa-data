from datetime import datetime
from psycopg2 import sql


def to_db(connection, table_name, data: list[dict]):
    loaded_count = 0
    failed_count = 0

    for entity in data:
        keys = list(entity.keys())
        values = list(entity.values())

        query = sql.SQL("INSERT INTO {table} ({keys}) VALUES ({value_placeholders});").format(
                table=sql.Identifier(table_name),
                keys=sql.SQL(', ').join(map(sql.Identifier, keys)),
                value_placeholders=sql.SQL(', ').join(sql.Placeholder() * len(keys)))

        try:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                connection.commit()
                loaded_count += 1
        
        except Exception as e:
            print(f'Write to database failed with error:\n    {e}')
            continue

    to_console(f'Wrote data for {loaded_count} vehicles to database with {failed_count} failures')


def to_console(str):
    now = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    print(f'{now}   {str}')