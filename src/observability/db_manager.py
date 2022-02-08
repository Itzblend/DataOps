import json
import os
import sys

from src.paths.paths import DDL_PATH, METADATA_PATH, POSTGRES_SCHEMAS_PATH
from .collectors import DatabaseCollector
import shutil

from io import StringIO


from ..etl.github.db import Database

DEPLOYMENT = os.environ.get('DEPLOYMENT') or 'DEV'
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s-[%(filename)s:%(lineno)s - %(funcName)1s()]-%(levelname)s: %(message)s',
                    handlers=[
                        logging.FileHandler(f'{sys.argv[0].split(".")[0]}.log'),
                        logging.StreamHandler()
                    ])


class SchemaManager:
    def __init__(self, db_config):
        self.db_config = db_config
        pass

    def newline_json_generator(self, json_file):
        with open(json_file, 'r', encoding='utf-8') as schema_file:
            for schema_line in schema_file.readlines():
                yield json.loads(schema_line)

    def get_postgres_default_tables(self) -> list:
        with open(f'{METADATA_PATH}/postgres_default_tables.txt', 'r', encoding='utf-8') as postgres_default_tables_file:
            postgres_default_tables = postgres_default_tables_file.read()
        return postgres_default_tables

    def generate_postgres_ddl(self, database: str):
        # Create Database collector instance and create newline json file from information_schema.columns
        db_collector = DatabaseCollector(database=database, db_config=self.db_config)
        db_collector.collect_postgres_schemas()

        shutil.rmtree(DDL_PATH, ignore_errors=True)
        os.makedirs(DDL_PATH, exist_ok=True)

        database_schema_folders = (set([directory for a, directory, c in os.walk('postgres_schemas')
                                        if directory][0]))

        # Read the default postgres tables to weed them out from processing
        with open(f'{METADATA_PATH}/postgres_default_tables.txt', 'r', encoding='utf-8') as postgres_default_tables_file:
            postgres_default_tables = postgres_default_tables_file.read()

        for database_schema_folder in database_schema_folders:
            print(database_schema_folder)

            db_schema_table_tuple = set([(row["table_catalog"], row["table_schema"], row["table_name"]) for row in
                                         self.newline_json_generator(
                                             f'postgres_schemas/{database_schema_folder}/tables.json') if
                                         row["table_name"] not in postgres_default_tables])

            for db, schema, table in db_schema_table_tuple:
                # Create StringIO instance for the table ddl, writing the ddl row by row and at the end, writing it to file
                ddl_string = StringIO()

                columns_and_types = [(row["column_name"], row["data_type"], row["column_default"]) for row in
                                     self.newline_json_generator(
                                         f'postgres_schemas/{database_schema_folder}/columns.json') if
                                     row["table_name"] == table]

                try:
                    primary_keys = [row["column_name"] for row in
                                    self.newline_json_generator(
                                        f'postgres_schemas/{database_schema_folder}/primary_keys.json')
                                    if row["table_name"] == table]
                except FileNotFoundError:
                    primary_keys = []

                ddl_string.write(f'CREATE TABLE IF NOT EXISTS {db}.{schema}.{table} (\n')
                for col_type in columns_and_types:
                    if col_type == columns_and_types[-1] and not primary_keys:
                        ddl_string.write(f'\t{col_type[0]} {col_type[1]}')
                        if col_type[2] is not None:
                            ddl_string.write(f' DEFAULT {col_type[2]}\n')
                        else:
                            ddl_string.write('\n')

                    else:
                        ddl_string.write(f'\t{col_type[0]} {col_type[1]},\n')

                if primary_keys:
                    ddl_string.write(f'\tPRIMARY KEY({", ".join(primary_keys)})\n')
                ddl_string.write(')')

                ddl_filepath = f'{DDL_PATH}/{db}_{schema}_{table}_t.sql'
                with open(ddl_filepath, 'w') as table_ddl_file:
                    table_ddl_file.write(ddl_string.getvalue())
        logging.info(f'DDL data written to {DDL_PATH}')

    def generate_postgres_schema_configs(self, database: str):
        # Create Database collector instance and create newline json file from information_schema.columns
        db_collector = DatabaseCollector(database=database, db_config=self.db_config)
        db_collector.collect_postgres_schemas()

        # DB instance to load schema data
        db = Database(host=self.db_config["host"], port=self.db_config["port"],
                      user=self.db_config["user"], password=self.db_config["password"],
                      database='metadata')

        # Read the default postgres tables to weed them out from processing
        with open(f'{METADATA_PATH}/postgres_default_tables.txt', 'r', encoding='utf-8') as postgres_default_tables_file:
            postgres_default_tables = postgres_default_tables_file.read()

        # Creating a primary key dictionary to use later
        primary_keys = {}
        try:
            for row in self.newline_json_generator('postgres_schemas/github_primary_keys.json'):
                if row["table_name"] not in primary_keys.keys():
                    primary_keys[row["table_name"]] = []
                    primary_keys[row["table_name"]].append(row["column_name"])
                else:
                    primary_keys[row["table_name"]].append(row["column_name"])
        except FileNotFoundError:
            pass

        database_schema_folders = (set([directory for a, directory, c in os.walk('postgres_schemas')
                                        if directory][0]))

        for database_schema_folder in database_schema_folders:
            db_schema_table_tuple = set([(row["table_catalog"], row["table_schema"], row["table_name"]) for row in
                                         self.newline_json_generator(
                                             f'postgres_schemas/{database_schema_folder}/tables.json') if
                                         row["table_name"] not in postgres_default_tables])

            for database, schema, table in db_schema_table_tuple:
                table_configuration = {}
                col_type_default_tuple = [
                    (row["column_name"], row["data_type"],
                     row["column_default"]) for row in
                    self.newline_json_generator(f'postgres_schemas/{database_schema_folder}/columns.json') if
                    row["table_name"] == table]

                table_configuration["database"] = database
                table_configuration["schema"] = schema
                table_configuration["table_name"] = table
                table_configuration["columns"] = {}
                table_configuration["defaults"] = {}
                for row in col_type_default_tuple:
                    table_configuration["columns"][row[0]] = row[1]
                    if row[2] is not None:
                        table_configuration["defaults"][row[0]] = row[2]

                table_configuration["constraints"] = {}
                if table in primary_keys.keys():
                    table_configuration["constraints"]["primary_key"] = primary_keys[table]
                else:
                    table_configuration["constraints"]["primary_key"] = []

                with db.connection_cursor() as cur:
                    cur.execute("""
                        INSERT INTO observability.schemas(database_name, schema_name, table_name, config)
                        VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING
                    """, (database, schema, table, json.dumps(table_configuration)))
                    logging.info(f"Inserted schema data of {database}.{schema}.{table} into the metadata database")


    def detect_dropped_tables(self, database: str):
        # Cross reference 'information_schema.tables' table and metadata database for dropped tables
        db_collector = DatabaseCollector(database=database, db_config=self.db_config)
        db_collector.collect_postgres_schemas()

        # DB instance to load schema data
        db = Database(host=self.db_config["host"], port=self.db_config["port"],
                      user=self.db_config["user"], password=self.db_config["password"],
                      database='metadata')

        postgres_default_tables = self.get_postgres_default_tables()

        #database_schema_folders = (set([directory for a, directory, c in os.walk('postgres_schemas')
        #                                if directory][0]))

        database_schema_folder = f'{POSTGRES_SCHEMAS_PATH}/{database}'

        db_schema_table_tuple = set([(row["table_catalog"], row["table_schema"], row["table_name"]) for row in
                                     self.newline_json_generator(
                                         f'{database_schema_folder}/tables.json') if
                                     row["table_name"] not in postgres_default_tables])

        with db.connection_cursor() as cur:
            cur.execute(f"""
                SELECT DISTINCT ON (database_name, schema_name, table_name) database_name, schema_name, table_name
                FROM observability.schemas
                WHERE database_name = '{database_schema_folder}';
            """)

            metadata_tables = cur.fetchall()

        deleted_db_schema_table_set = set(metadata_tables) - db_schema_table_tuple
        if deleted_db_schema_table_set:
            for deleted_db_schema_table in deleted_db_schema_table_set:
                with db.connection_cursor() as cur:
                    # Selects the latest row of deleted table and inserting it with exists value changed to FALSE
                    cur.execute(f"""
                        INSERT INTO observability.schemas 
                        SELECT DISTINCT ON (database_name, schema_name, table_name) 
                        database_name, schema_name, table_name, config, now() at time zone 'utc', 
                        CASE WHEN exists = TRUE THEN FALSE ELSE TRUE END
                        FROM observability.schemas
                        WHERE database_name = '{deleted_db_schema_table[0]}' AND schema_name = '{deleted_db_schema_table[1]}'
                        AND table_name = '{deleted_db_schema_table[2]}'
                        ORDER BY database_name, schema_name, table_name, discovery_time DESC

                    """)
                    if cur.rowcount > 0:
                        logging.info(
                            f'Marked {cur.rowcount} tables as dropped in database {deleted_db_schema_table[0]}')

if __name__ == '__main__':
    pass
    #config = ConfigParser(os.environ)
    #config.read('../config.ini')
    #db_config = config["database-local-dev"]

    #sm = SchemaManager()
    # sm.generate_postgres_ddl(schema_dir='postgres_schemas', database='github')
    # sm.generate_postgres_schema_configs(database='github')
    #sm.detect_dropped_tables(database='github')

# TODO: Monitor table schema changes by queries (check columns periodically)