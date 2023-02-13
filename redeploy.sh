#!/bin/bash


# build new docker image
docker build --tag kraken:latest .

# restart the containers with the new image
docker-compose up --force-recreate scylla charybdis flower