#!/bin/bash

docker-compose build --no-cache
chmod 666 /var/run/docker.sock
docker-compose up

