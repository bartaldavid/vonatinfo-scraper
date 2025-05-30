import datetime
import os
import sqlite3
import libsql_experimental as libsql
import requests
import apscheduler.schedulers.blocking as blocking_scheduler
import logging
import threading
import time

DB_PATH = os.getenv("DB_URL", "tmp/temp.db")
logging.basicConfig(level=logging.INFO)


def fetch_data():
    response = requests.post(
        "https://vonatinfo.mav-start.hu/map.aspx/getData",
        json={"a": "TRAINS", "jo": {"history": False, "id": False}},
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def initialize_db():
    with open("initial-schema.sql") as schema:
        stmt = schema.read()
        db_conn = sqlite3.connect(DB_PATH)
        db_conn.executescript(stmt)
        db_conn.commit()
        db_conn.close()


def save_to_db(data):
    result = data.get("d", {}).get("result", {})
    if not result:
        logging.warning("No result found in the data.")
        return

    created_at = result.get("@CreationTime", None)
    trains = result.get("Trains", {}).get("Train", [])

    timestamp_unix = datetime.datetime.strptime(
        created_at, r"%Y.%m.%d %H:%M:%S"
    )

    records = [
        (
            timestamp_unix.timestamp(),
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

    if len(records) == 0:
        logging.info("No records to save")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.executemany(
        """
                INSERT INTO train_position (
                    created_at, delay, lat, lon, line, relation, menetvonal, elvira_id, train_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
        records,
    )
    conn.commit()
    conn.close()
    logging.info(f"{len(records)} records saved to database successfully.")


def job():
    data = fetch_data()
    if data is None:
        logging.warning("Failed to fetch data from the API.")
        return
    logging.info("Data fetched successfully from the API.")

    start = time.time()
    save_to_db(data)
    logging.info(f"Saving took {time.time() - start}")

    logging.info("Data saved to database.")


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
