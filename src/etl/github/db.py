import psycopg2
from configparser import ConfigParser
from contextlib import contextmanager
import os
import sys
import json
import logging

from src.paths.paths import ETL_DATA_PATH

DEPLOYMENT = os.environ.get("DEPLOYMENT") or "DEV"
DATA_FOLDER = ETL_DATA_PATH
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s-[%(filename)s:%(lineno)s - %(funcName)1s()]-%(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(f'{sys.argv[0].split(".")[0]}.log'),
        logging.StreamHandler(),
    ],
)


class Database:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.test_connection()

    def test_connection(self):
        try:
            conn = psycopg2.connect(self.connection_string)
            logging.info("Connection successful")
            conn.close()
        except psycopg2.OperationalError:
            logging.error("Failed to connect to database")

    @contextmanager
    def connection_cursor(self):
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except psycopg2.DatabaseError:
            logging.error("Database operation failed:", exc_info=True, stack_info=True)
            raise
        finally:
            cur.close()
            conn.close()

    def load_json_files(self, data_dir: str, schema: str, table: str):
        """
        Inserts new-line delimited JSON data from file to database
        """
        # Collecting json files
        json_files = self.collect_json_files(data_dir)

        for file in json_files:
            # Creating connection to database and copying data into the datalake schema
            conn = psycopg2.connect(self.connection_string)
            cur = conn.cursor()
            with open(file, "r") as insertion_file:
                # insertion_data = json.load(insertion_file)
                try:
                    cur.copy_expert(
                        f"COPY {schema}.{table} FROM STDIN WITH CSV quote e'\x01' delimiter e'\x02'",
                        insertion_file,
                    )
                    conn.commit()
                except psycopg2.DatabaseError:
                    logging.error("Database operation failed")
                    raise
                finally:
                    cur.close()
                    conn.close()

    def collect_json_files(self, directory):
        json_files = []
        for dirpath, _, files in os.walk(directory):
            for filename in files:
                if filename.endswith(".json"):
                    json_files.append(os.path.join(dirpath, filename))

        return json_files


if __name__ == "__main__":
    pass
