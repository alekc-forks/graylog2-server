#!/usr/bin/env bash
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin || travis_terminate 1;
docker build -t alekcander/graylog-patched . || travis_terminate 1;
docker push alekcander/graylog-patched
