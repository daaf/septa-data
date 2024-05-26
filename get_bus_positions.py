from apscheduler.schedulers.blocking import BlockingScheduler
from config import load_config
from connect import connect_to_db
from utils import write_to_db, write_to_console
from queries import get_vehicle_positions


def main():
    scheduler = BlockingScheduler()
    job = scheduler.add_job(execute, 'interval', minutes=1)
    scheduler.start()


def execute():
    feed = get_vehicle_positions("bus")
    vehicles = parse_vehicle_position_feed(feed)
    connection = connect_to_db()
    table_name = load_config(section="tables")["bus_trolley_pos"]

    write_to_db(connection, table_name, data=vehicles)
    connection.close()


def parse_vehicle_position_feed(feed):
    vehicles = []
    vehicle_count = 0

    for entity in feed:
        id = entity.id
        vehicle_count += 1

        if id == "None":
            id = None

        longitude =  entity.vehicle.position.longitude
        latitude = entity.vehicle.position.latitude

        vehicle = {
            "id": id,
            "timestamp": entity.vehicle.timestamp,
            "position": f'POINT({longitude} {latitude})',
            "bearing": entity.vehicle.position.bearing,
            "trip_id": entity.vehicle.trip.trip_id,
        }

        vehicles.append(vehicle)

    write_to_console(f'Fetched data for {vehicle_count} vehicles')

    return vehicles


if __name__ == "__main__":
    main()
