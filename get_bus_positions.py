from datetime import datetime
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
    table_name = load_config(section="bus_trolley_positions")["table"]

    write_to_db(connection, table_name, data=vehicles)
    connection.close()


def parse_vehicle_position_feed(feed):
    vehicles = []
    vehicle_count = 0

    for entity in feed:
        vehicle_count += 1

        timestamp = datetime.fromtimestamp(
                entity.vehicle.timestamp).strftime("%Y-%m-%d, %H:%M:%S")
        vehicle_id = entity.id
        latitude = entity.vehicle.position.latitude
        longitude = entity.vehicle.position.longitude
        bearing = entity.vehicle.position.bearing
        
        vehicle_position = {
            "timestamp": timestamp,
            "vehicle_id": vehicle_id if vehicle_id != "None" else None,
            "latitude": latitude if latitude != 0 else None,
            "longitude": longitude if longitude != 0 else None,
            "bearing": bearing,
            "trip_id": entity.vehicle.trip.trip_id,
        }

        vehicles.append(vehicle_position)

    write_to_console(f'Fetched data for {vehicle_count} vehicles')

    return vehicles


if __name__ == "__main__":
    main()
