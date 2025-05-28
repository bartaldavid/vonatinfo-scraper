import libsql_client
import requests
import apscheduler.schedulers.blocking as blocking_scheduler
import logging

API_URL = "https://vonatinfo.mav-start.hu/map.aspx/getData"
DB_URL = "file:test.db"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


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


def save_to_db(data):
    try:
        with libsql_client.create_client_sync(DB_URL) as client:
            client.execute("""
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

            result = data.get("d", {}).get("result", {})
            if not result:
                logging.warning("No result found in the data.")
                return

            created_at = result.get("@CreationTime", None)

            for train in result.get("Trains", {}).get("Train", []):
                try:
                    client.execute(
                        """
                        INSERT INTO train_position (
                            creation_time, delay, lat, lon, line, relation, menetvonal, elviraid, trainnumber
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
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
                        ),
                    )
                except Exception as e:
                    logging.error(f"Error inserting train data: {e}")
    except Exception as e:
        logging.error(f"Database error: {e}")


def job():
    data = fetch_data()
    if data is None:
        logging.warning("Failed to fetch data from the API.")
        return
    save_to_db(data)
    logging.info("Data fetched and saved to database.")


if __name__ == "__main__":
    logging.info("Starting the train position data fetcher...")
    job()  # Run the job once at startup
    scheduler = blocking_scheduler.BlockingScheduler()
    scheduler.add_job(job, "interval", seconds=20)
    try:
        logging.info("Scheduler started. Fetching data every 20 seconds.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped by user.")
