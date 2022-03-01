CREATE ROLE builderbob LOGIN SUPERUSER PASSWORD 'localdev';
CREATE
DATABASE metadata;
\c
metadata;
CREATE SCHEMA IF NOT EXISTS observability;
CREATE TABLE observability.schemas
(
    database_name VARCHAR,
    schema_name   VARCHAR,
    table_name    VARCHAR,
    config        JSONB,
    timestamp     timestamp default (now() at time zone 'utc')
);

CREATE
DATABASE github;

\c
github;

CREATE SCHEMA IF NOT EXISTS datalake;
SET
SEARCH_PATH TO datalake;
CREATE TABLE issues_json
(
    data JSON
);
CREATE UNIQUE INDEX issues_json_unique ON issues_json ((data - >> 'repository_url'), (data ->> 'id'), (data ->> 'updated_at'));

CREATE TABLE pulls_json
(
    data JSON
);
CREATE TABLE commits_json
(
    data JSON
);
CREATE TABLE org_events_json
(
    data json
);
CREATE TABLE events_json
(
    data json
);
