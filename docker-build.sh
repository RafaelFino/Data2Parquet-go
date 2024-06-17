#!/bin/bash
docker run -v ./:/go/src --entrypoint 'make' -w /go/src golang:1.20 