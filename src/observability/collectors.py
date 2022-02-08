from configparser import ConfigParser
import os
import sys

from src.paths.paths import POSTGRES_SCHEMAS_PATH
from ..etl.github.db import Database

from ..utils.fileio import FileIO

DEPLOYMENT = os.environ.get('DEPLOYMENT') or 'DEV'
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s-[%(filename)s:%(lineno)s - %(funcName)1s()]-%(levelname)s: %(message)s',
                    handlers=[
                        logging.FileHandler(f'{sys.argv[0].split(".")[0]}.log'),
                        logging.StreamHandler()
                    ])


class HttpCollector:
    def __init__(self):
        pass


class DatabaseCollector:
    def __init__(self, db_config, database):
        self.db_config = db_config
        self.database = database

    def collect_postgres_schemas(self):

        db = Database(host=self.db_config["host"], port=self.db_config["port"],
                      user=self.db_config["user"], password=self.db_config["password"],
                      database=self.database)

        with db.connection_cursor() as cur:
            # Columns
            cur.execute("""
                SELECT * FROM information_schema.columns;
            """)

            outfile = FileIO(f'{POSTGRES_SCHEMAS_PATH}/{self.db_config["database"]}/columns.json', create_parent_dir=True)

            logging.info(f'Writing {self.db_config["database"]} schema to {outfile.filepath}')
            for row in self.postgres_cursor_to_dictionary(cur):
                outfile.newline_dict_to_file(row)

            # Primary keys
            with open('src/observability/sql/get_primary_keys.sql', 'r', encoding='utf-8') as get_primary_keys_query_file:
                cur.execute(get_primary_keys_query_file.read())

            primary_keys_outfile = FileIO(f'{POSTGRES_SCHEMAS_PATH}/{self.db_config["database"]}/primary_keys.json',
                                          create_parent_dir=False)

            logging.info(f'Writing {self.db_config["database"]} primary keys to {primary_keys_outfile.filepath}')
            for row in self.postgres_cursor_to_dictionary(cur):
                primary_keys_outfile.newline_dict_to_file(row)

            # Tables
            cur.execute("""
                            SELECT * FROM information_schema.tables;
                        """)

            tables_outfile = FileIO(f'{POSTGRES_SCHEMAS_PATH}/{self.db_config["database"]}/tables.json',
                                          create_parent_dir=False)

            logging.info(f'Writing {self.db_config["database"]} tables to {tables_outfile.filepath}')
            for row in self.postgres_cursor_to_dictionary(cur):
                tables_outfile.newline_dict_to_file(row)


    def postgres_cursor_to_dictionary(self, cursor):
        cols = [col[0] for col in cursor.description]
        for row in cursor.fetchall():
            temp_dict = {}
            for col, value in zip(cols, row):
                temp_dict[col] = value
            yield temp_dict


if __name__ == '__main__':


    db_collector = DatabaseCollector()
    db_collector.collect_postgres_schemas()
