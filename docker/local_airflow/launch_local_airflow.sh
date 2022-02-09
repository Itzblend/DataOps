#!/bin/bash

docker-compose up -d
sleep 60
docker exec local_airflow_webserver_1 airflow users create -e buildberbob@bobbuilder123123.com -f bob -l builder -r Admin -u builderbob -p builderbob
