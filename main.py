import os
import sqlite3
import libsql_experimental as libsql
import requests
import apscheduler.schedulers.blocking as blocking_scheduler
import logging
import threading
import time

API_URL = "https://vonatinfo.mav-start.hu/map.aspx/getData"
url = os.getenv("TURSO_DATABASE_URL")
auth_token = os.getenv("TURSO_AUTH_TOKEN")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Global DB connection
db_conn: sqlite3.Connection | None = None


def fetch_data():
    try:
        response = requests.post(
            API_URL,
            json={"a": "TRAINS", "jo": {"history": False, "id": False}},
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def initialize_db():
    global db_conn
    try:
        db_conn = libsql.connect(  # type: ignore
            url,
            auth_token=auth_token,
        )
        db_conn.execute("""
            CREATE TABLE IF NOT EXISTS train_position (
                creation_time TEXT,
                delay REAL,
                lat REAL,
                lon REAL,
                line TEXT,
                relation TEXT,
                menetvonal TEXT,
                elviraid TEXT,
                trainnumber TEXT
            )
        """)
        db_conn.commit()
        logging.info("Database initialized successfully.")
    except Exception as e:
        logging.error(f"Database initialization error: {e}")


def save_to_db(data):
    try:
        result = data.get("d", {}).get("result", {})
        if not result:
            logging.warning("No result found in the data.")
            return

        created_at = result.get("@CreationTime", None)
        trains = result.get("Trains", {}).get("Train", [])

        records = [
            (
                created_at,
                train.get("@Delay"),
                train.get("@Lat"),
                train.get("@Lon"),
                train.get("@Line"),
                train.get("@Relation"),
                train.get("@Menetvonal"),
                train.get("@ElviraID"),
                train.get("@TrainNumber"),
            )
            for train in trains
        ]

        if records and db_conn is not None:
            db_conn.executemany(
                """
                INSERT INTO train_position (
                    creation_time, delay, lat, lon, line, relation, menetvonal, elviraid, trainnumber
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                records,
            )
            db_conn.commit()
            logging.info(f"{len(records)} records saved to database successfully.")
        else:
            logging.info("No train records to save.")
    except Exception as e:
        logging.error(f"Database error: {e}")


def job():
    data = fetch_data()
    if data is None:
        logging.warning("Failed to fetch data from the API.")
        return
    logging.info("Data fetched successfully from the API.")
    save_to_db(data)
    logging.info("Data fetched and saved to database.")


def stop_scheduler_after_delay(scheduler, delay_seconds):
    def stopper():
        time.sleep(delay_seconds)
        logging.info("Stopping scheduler after 2 days.")
        scheduler.shutdown()

    t = threading.Thread(target=stopper, daemon=True)
    t.start()


if __name__ == "__main__":
    logging.info("Starting the train position data fetcher...")
    initialize_db()
    job()  # Run the job once at startup
    scheduler = blocking_scheduler.BlockingScheduler()
    scheduler.add_job(job, "interval", seconds=10, max_instances=2)
    # Stop after 2 days (172800 seconds)
    stop_scheduler_after_delay(scheduler, 172800)
    try:
        logging.info("Scheduler started. Fetching data every 10 seconds.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped by user.")
    finally:
        if db_conn:
            db_conn.close()
            logging.info("Database connection closed.")
