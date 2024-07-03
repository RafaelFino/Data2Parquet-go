#!/bin/bash
docker buildx build docker/golang-builder/ -t golang-builder
docker run \
    -v ./:/go/src \
    --rm \
    --entrypoint make \
    golang-builder linux
