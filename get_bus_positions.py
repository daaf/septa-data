from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from config import load_config
from connect import connect_to_db
import write
from queries import get_vehicle_positions


def main():
    config = load_config(section="scheduler")
    interval_minutes = int(config["interval_minutes"])
    start_datetime = config["start_datetime"]
    end_datetime = config["end_datetime"]

    scheduler = BlockingScheduler()
    job = scheduler.add_job(execute, 'interval', 
                            minutes=interval_minutes,
                            start_date=start_datetime,
                            end_date=end_datetime)
    
    scheduler.start()


def execute():
    connection = connect_to_db()
    feed = get_vehicle_positions("bus")
    vehicles = parse_vehicle_position_feed(feed)
    table_name = load_config(section="tables")["bus_trolley_positions"]
    
    write.to_db(connection, table_name, data=vehicles)
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
            "route_id": entity.vehicle.trip.route_id,
        }

        vehicles.append(vehicle_position)

    write.to_console(f'Fetched data for {vehicle_count} vehicles')

    return vehicles


if __name__ == "__main__":
    main()
