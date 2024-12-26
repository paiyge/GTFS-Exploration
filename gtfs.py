import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def connect_to_database(db_url):
    """
    Connect to the PostgreSQL database and return an engine.

    Args:
    - db_url (str): The connection string for the PostgreSQL database.

    Returns:
    - engine: A SQLAlchemy engine instance.
    """
    try:
        engine = create_engine(db_url)
        conn = engine.connect()
        print("Database connection successful!")
        conn.close()
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def load_gtfs_to_postgres(gtfs_folder, engine):
    """
    Load GTFS files into a PostgreSQL database.

    Args:
    - gtfs_folder (str): Path to the folder containing GTFS files.
    - engine: SQLAlchemy engine instance.
    """
    if engine is None:
        print("Engine not connected. Exiting.")
        return

    files = ['stops', 'routes', 'trips', 'stop_times', 'calendar']
    for file in files:
        filepath = f"{gtfs_folder}/{file}.txt"
        df = pd.read_csv(filepath)
        table_name = file.lower()
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Loaded {file} into PostgreSQL as {table_name}.")


def analyze_gtfs_data(db_url):
    """
    Analyze GTFS data to summarize transit system metrics.

    Args:
    - db_url (str): Database URL for PostgreSQL.
    """
    engine = create_engine(db_url)

    # Query stop and route data
    stops_q = pd.read_sql("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops", engine)
    routes_q = pd.read_sql("SELECT route_id, route_short_name, route_long_name FROM routes", engine)

    # Join stops with trips to calculate stop frequency
    query = """
    SELECT s.stop_id, s.stop_name, COUNT(st.trip_id) AS trip_count
        FROM stops s
        JOIN stop_times st ON s.stop_id = st.stop_id
        GROUP BY s.stop_id, s.stop_name
        ORDER BY trip_count DESC
    """
    stop_frequencies_q = pd.read_sql(query, engine)

    print("Top Stops by Frequency:")
    print(stop_frequencies_q.head())
    return stops_q, routes_q, stop_frequencies_q

# Main


db_url = "postgresql://postgres:password@localhost:5432/gtfs_db"
engine = connect_to_database(db_url)

if engine:
    gtfs_folder = "/Users/paigenorris/gtfs/GTFS-MEP/acadia_data"
    # load_gtfs_to_postgres(gtfs_folder, engine)

# Analyze GTFS Data
stops, routes, stop_frequencies = analyze_gtfs_data(db_url)