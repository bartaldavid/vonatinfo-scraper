import sqlite3
import logging
import datetime
import zoneinfo
import os

DB_PATH = os.getenv("DB_URL", "tmp/temp.db")


def initialize_db():
    with open("initial-schema.sql") as schema:
        stmt = schema.read()
        db_conn = sqlite3.connect(DB_PATH)
        db_conn.executescript(stmt)
        db_conn.commit()
        db_conn.close()


def get_or_create_id(conn, table, column, value):
    if value is None:
        return None
    conn.execute(f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?)", (value,))
    row = conn.execute(
        f"SELECT id FROM {table} WHERE {column} = ?", (value,)
    ).fetchone()
    return row[0] if row else None


def save_to_db(data):
    result = data.get("d", {}).get("result", {})
    if not result:
        logging.warning("No result found in the data.")
        return

    created_at = result.get("@CreationTime", None)
    trains = result.get("Trains", {}).get("Train", [])

    if not created_at:
        logging.warning("No creation time found in the data.")
        return

    created_at_dt = datetime.datetime.strptime(
        created_at, r"%Y.%m.%d %H:%M:%S"
    ).astimezone(zoneinfo.ZoneInfo("Europe/Budapest"))

    if not trains:
        logging.info("No records to save")
        return

    conn = sqlite3.connect(DB_PATH)
    saved_count = 0
    for train in trains:
        try:
            lat = train.get("@Lat")
            lon = train.get("@Lon")
            lat_micro = int(round(lat * 1e6)) if lat is not None else None
            lon_micro = int(round(lon * 1e6)) if lon is not None else None
            record = {
                "created_at": int(created_at_dt.timestamp()),
                "delay": train.get("@Delay"),
                "lat_micro": lat_micro,
                "lon_micro": lon_micro,
                "line": train.get("@Line"),
                "relation": train.get("@Relation"),
                "menetvonal": train.get("@Menetvonal"),
                "elvira_id": train.get("@ElviraID"),
                "train_number": train.get("@TrainNumber"),
            }
            line_id = get_or_create_id(conn, "line_id", "line", record["line"])
            relation_id = get_or_create_id(conn, "relation", "name", record["relation"])
            menetvonal_id = get_or_create_id(
                conn, "menetvonal", "name", record["menetvonal"]
            )
            elvira_id_id = get_or_create_id(
                conn, "elvira_id", "elvira_id", record["elvira_id"]
            )
            train_number_id = get_or_create_id(
                conn, "train_number", "train_number", record["train_number"]
            )
            conn.execute(
                """
                INSERT INTO train_position (
                    created_at, delay, lat_micro, lon_micro, elvira_id_id, menetvonal_id, line_id, relation_id, train_number_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["created_at"],
                    record["delay"],
                    record["lat_micro"],
                    record["lon_micro"],
                    elvira_id_id,
                    menetvonal_id,
                    line_id,
                    relation_id,
                    train_number_id,
                ),
            )
            saved_count += 1
        except Exception as e:
            logging.warning(f"Error processing/saving train record: {e}")

    conn.commit()
    conn.close()
    logging.info(f"{saved_count} records saved to database successfully.")
