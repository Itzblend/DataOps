CREATE DATABASE github;

\c github;

CREATE SCHEMA IF NOT EXISTS datalake;
SET SEARCH_PATH TO datalake;
CREATE TABLE issues_json (data JSON);
CREATE TABLE pulls_json (data JSON);
CREATE TABLE commits_json (data JSON);
