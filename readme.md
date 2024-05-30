# septa-data

## Setup

### Create database
```
CREATE DATABASE septa;
```

### Create table
```
CREATE TABLE bus_trolley_positions (
    timestamp timestamp,
    vehicle_id integer,
    latitude decimal(8,6),
    longitude decimal(9,6),
    bearing decimal(9,6),
    trip_id integer,
    route_id VARCHAR(64)
);
```
```
CREATE VIEW vehicles_on_route AS
SELECT route_id, COUNT(DISTINCT trip_id) AS vehicles_on_route
FROM bus_trolley_positions
GROUP BY route_id
ORDER BY vehicles_on_route DESC;
```

## More info
Refer here for info on SEPTA bus and trolley IDs: https://philadelphiatransitvehicles.info/blocks/