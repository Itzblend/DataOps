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

if CODE_ENV in ["STAGING", "PROD"]:
    login_resp = json.loads(os.popen("""curl -s \
                           --request POST \
                           --data '{"password": "'"$DATAOPS_BOB_VAULT_PASS"'"}' \
                           ${VAULT_ADDR}/v1/auth/userpass/login/dataops_bob""").read())
    os.environ["VAULT_TOKEN"] = login_resp["auth"]["client_token"]


    db_secrets = json.loads(os.popen(f"""curl -s \
                            --request GET \
                            -H "X-Vault-Token: {os.environ["VAULT_TOKEN"]}" \
                            {os.environ["VAULT_ADDR"]}/v1/dataops_staging/data/database""").read())

    os.environ["DATAOPS_STAGING_DB_PASS"] = db_secrets["data"]["data"]["password"]
    os.environ["DATAOPS_STAGING_DB_HOST"] = db_secrets["data"]["data"]["host"]


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


@main.command()
@click.option('--database', default='github')
def fetch_org_events(database: str):
    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG, database=database)
    etl.fetch_org_events(save_folder='org_events')


@main.command()
@click.option('--database')
def load_org_events(database: str):
    db = Database(host=DB_CONFIG["host"], port=DB_CONFIG["port"],
                  user=DB_CONFIG["user"], password=DB_CONFIG["password"],
                  database=database)

    db.load_json_files(data_dir=f'{ETL_DATA_PATH}/org_events', schema='datalake', table='org_events_json')


@main.command()
def list_repos():
    etl = ETL('dbt-labs', config=CONFIG, db_config=DB_CONFIG)
    etl.list_org_repos()

