#!/bin/bash

vault login -no-print -method userpass username=dataops_bob password=${DATAOPS_BOB_VAULT_PASS}

export DATAOPS_STAGING_DB_USER=$(vault kv get -field=user Dataops/database)
export DATAOPS_STAGING_DB_PASS=$(vault kv get -field=password Dataops/database)

#echo $DATAOPS_STAGING_DB_USER
#echo $DATAOPS_STAGING_DB_PASS

docker-compose -f staging.yml up -d
