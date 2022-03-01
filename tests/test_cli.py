from src.observability.db_manager import SchemaManager
from src.observability.collectors import DatabaseCollector
from src.etl.github.etl import ETL
from src.etl.github.db import Database
from configparser import ConfigParser
import os

from src.paths.paths import ETL_DATA_PATH


def _get_db_config():
    config = ConfigParser(os.environ)
    config.read('./config.ini')
    db_config = config["database-local-dev"]

    return db_config, config

DB_CONFIG, CONFIG = _get_db_config()
database = 'github'


def test_generate_postgres_ddl():
    sm = SchemaManager(db_config=DB_CONFIG)
    sm.generate_postgres_ddl(database=database)


def test_generate_postgres_schema_configs():
    sm = SchemaManager(db_config=DB_CONFIG)
    sm.generate_postgres_schema_configs(database=database)


def test_detect_dropped_tables():
    # Maybe add here process to create and delete table to test functionality
    sm = SchemaManager(db_config=DB_CONFIG)
    sm.detect_dropped_tables(database=database)


def test_collect_postgres_schemas():
    db_collector = DatabaseCollector(database=database, db_config=DB_CONFIG)
    db_collector.collect_postgres_schemas()


def test_fetch_issues():
    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG, database=database)
    etl.fetch_issues(save_folder='issues')


#def test_fetch_pulls():
#    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG, database=database)
#    etl.fetch_pulls(save_folder='pulls')


def test_fetch_commits():
    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG, database=database)
    etl.fetch_commits(save_folder='commits')


def test_load_github_issues():
    db = Database(host=DB_CONFIG["host"], port=DB_CONFIG["port"],
                 user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                 database=database)

    db.load_json_files(data_dir=f'{ETL_DATA_PATH}/issues', schema='datalake', table='issues_json')



#def test_load_github_pulls():
#    db = Database(host=DB_CONFIG["host"], port=DB_CONFIG["port"],
#                 user=DB_CONFIG["user"], password=DB_CONFIG["password"],
#                 database=database)

#    db.load_json_files(data_dir=f'{ETL_DATA_PATH}/pulls', schema='datalake', table='pulls_json')


def test_load_github_commits():
    db = Database(host=DB_CONFIG["host"], port=DB_CONFIG["port"],
                  user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                  database=database)

    db.load_json_files(data_dir=f'{ETL_DATA_PATH}/commits', schema='datalake', table='commits_json')


def test_fetch_org_events():
    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG, database=database)
    etl.fetch_org_events(save_folder='org_events')


def test_load_org_events():
    db = Database(host=DB_CONFIG["host"], port=DB_CONFIG["port"],
                  user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                  database=database)

    db.load_json_files(data_dir=f'{ETL_DATA_PATH}/org_events', schema='datalake', table='org_events_json')
