import os
import apscheduler.schedulers.blocking as blocking_scheduler
import logging
import threading
import time
from src.db_file_server import start_db_file_server
from src.db import initialize_db, save_to_db
from src.api import fetch_data

DB_PATH = os.getenv("DB_URL", "tmp/temp.db")
logging.basicConfig(level=logging.INFO)


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


if __name__ == "__main__":
    logging.info("Starting the train position data fetcher...")
    # Start the DB file server in a background thread
    server_thread = threading.Thread(target=start_db_file_server, daemon=True)
    server_thread.start()
    initialize_db()
    job()  # Run the job once at startup
    scheduler = blocking_scheduler.BlockingScheduler()
    scheduler.add_job(job, "interval", seconds=30, max_instances=1)
    try:
        logging.info("Scheduler started. Fetching data every 30 seconds.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped by user.")
