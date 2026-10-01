import csv
import random
import sys
from datetime import datetime, timedelta

random.seed(42)

STATIONS = [
    ("EV001", "Burwood", -37.8490, 145.1140, 8),
    ("EV002", "Box Hill", -37.8189, 145.1230, 10),
    ("EV003", "Blackburn", -37.8200, 145.1500, 6),
    ("EV004", "Camberwell", -37.8420, 145.0570, 8),
    ("EV005", "Glen Waverley", -37.8790, 145.1640, 12),
    ("EV006", "Clayton", -37.9250, 145.1200, 10),
    ("EV007", "Richmond", -37.8230, 144.9980, 8),
    ("EV008", "Melbourne CBD", -37.8136, 144.9631, 16),
    ("EV009", "Hawthorn", -37.8220, 145.0350, 8),
    ("EV010", "Doncaster", -37.7870, 145.1250, 10),
]


def get_demand_factor(hour):
    if 7 <= hour <= 9:
        return 0.75, 1.00

    if 16 <= hour <= 19:
        return 0.80, 1.00

    if 10 <= hour <= 15:
        return 0.45, 0.80

    if 20 <= hour <= 22:
        return 0.40, 0.70

    return 0.10, 0.40


def generate_record(timestamp):
    station_id, suburb, lat, lon, total_ports = random.choice(STATIONS)

    minimum, maximum = get_demand_factor(timestamp.hour)
    occupancy_ratio = random.uniform(minimum, maximum)

    occupied_ports = int(round(total_ports * occupancy_ratio))
    occupied_ports = max(0, min(occupied_ports, total_ports))

    if occupied_ports >= total_ports:
        queue_length = random.randint(1, 8)
    elif occupancy_ratio >= 0.85:
        queue_length = random.randint(0, 4)
    else:
        queue_length = 0

    avg_session_minutes = random.randint(20, 75)

    return [
        timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        station_id,
        suburb,
        lat,
        lon,
        total_ports,
        occupied_ports,
        queue_length,
        avg_session_minutes,
    ]


def generate_file(filename, records):
    start_time = datetime(2026, 9, 1, 0, 0, 0)

    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow([
            "timestamp",
            "station_id",
            "suburb",
            "latitude",
            "longitude",
            "total_ports",
            "occupied_ports",
            "queue_length",
            "avg_session_minutes",
        ])

        for _ in range(records):
            timestamp = start_time + timedelta(
                seconds=random.randint(0, 30 * 24 * 60 * 60)
            )

            writer.writerow(generate_record(timestamp))

    print(f"Generated {records:,} EV charging records")
    print(f"Saved to: {filename}")


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python3 generate_ev_data.py "
            "<output_file> <number_of_records>"
        )
        sys.exit(1)

    filename = sys.argv[1]

    try:
        records = int(sys.argv[2])

        if records <= 0:
            raise ValueError

    except ValueError:
        print("Number of records must be a positive integer.")
        sys.exit(1)

    generate_file(filename, records)


if __name__ == "__main__":
    main()