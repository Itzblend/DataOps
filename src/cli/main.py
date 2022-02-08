import click
from configparser import ConfigParser
import os
from ..observability.db_manager import SchemaManager
from ..observability.collectors import DatabaseCollector
from ..etl.github.etl import ETL
from ..etl.github.db import Database
from ..paths.paths import ETL_DATA_PATH
import json

CODE_ENV = os.environ.get("DATAOPS_CODE_ENV") or "DEV"

code_env_dict = {
    "DEV": "database-local-dev",
    "STAGING": "database-staging",
    "PROD": "database-production"
}

VAULT_TOKEN = os.popen(
    f'vault login -format json -method userpass username=dataops_bob password={os.environ.get("DATAOPS_BOB_VAULT_PASS")} | jq ".auth.client_token"').read()
db_secrets = json.loads(os.popen('vault kv get -format=json dataops_staging/database').read())

os.environ["DATAOPS_STAGING_DB_PASS"] = db_secrets["data"]["data"]["password"]


@click.group()
def main():
    global DB_CONFIG
    DB_CONFIG = _get_db_config()


def _get_db_config():
    global CONFIG
    CONFIG = ConfigParser(os.environ)
    CONFIG.read('./config.ini')
    db_config = CONFIG[code_env_dict[CODE_ENV]]

    return db_config

@main.command()
@click.option('--database')
def generate_postgres_ddl(database: str):
    """
    Collects all table schemas from the selected database and generates DDL files into the schema_dir parameter
    """

    sm = SchemaManager(db_config=DB_CONFIG)
    sm.generate_postgres_ddl(database=database)

@main.command()
@click.option('--database')
def generate_postgres_schema_configs(database: str):
    """
    Collects all table schemas from the selected database and generates json file configs of the tables
    """

    sm = SchemaManager(db_config=DB_CONFIG)
    sm.generate_postgres_schema_configs(database=database)


@main.command()
@click.option('--database')
def detect_dropped_tables(database: str):

    sm = SchemaManager(db_config=DB_CONFIG)
    sm.detect_dropped_tables(database=database)


@main.command()
@click.option('--database')
def collect_postgres_schemas(database: str):

    db_collector = DatabaseCollector(database=database, db_config=DB_CONFIG)
    db_collector.collect_postgres_schemas()


@main.command()
@click.option('--database', default='github')
def fetch_issues(database: str):
    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG, database=database)
    etl.fetch_issues(save_folder='issues')

@main.command()
@click.option('--database', default='github')
def fetch_pulls(database: str):
    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG, database=database)
    etl.fetch_pulls(save_folder='pulls')


@main.command()
@click.option('--database')
def load_github_issues(database: str):
    db = Database(host=DB_CONFIG["host"], port=DB_CONFIG["port"],
                 user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                 database=database)

    db.load_json_files(data_dir=f'{ETL_DATA_PATH}/issues', schema='datalake', table='issues_json')


@main.command()
@click.option('--database')
def load_github_pulls(database: str):
    db = Database(host=DB_CONFIG["host"], port=DB_CONFIG["port"],
                 user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                 database=database)

    db.load_json_files(data_dir=f'{ETL_DATA_PATH}/pulls', schema='datalake', table='pulls_json')


@main.command()
@click.option('--database', default='github')
def fetch_commits(database: str):
    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG, database=database)
    etl.fetch_commits(save_folder='commits')


@main.command()
@click.option('--database')
def load_github_commits(database: str):
    db = Database(host=DB_CONFIG["host"], port=DB_CONFIG["port"],
                 user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                 database=database)

    db.load_json_files(data_dir=f'{ETL_DATA_PATH}/commits', schema='datalake', table='commits_json')