# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
FROM golang:alpine

# Contact maintainer with any issues you encounter
MAINTAINER Richard Knop <risoknop@gmail.com>

# Set environment variables
ENV PATH /go/bin:$PATH
ENV GOBIN /go/bin


RUN apk add --update make gcc g++

# Cd into the source code directory
WORKDIR /go/src/github.com/ehengao/machinery

# Copy the local package files to the container's workspace.
ADD . /go/src/github.com/ehengao/machinery

# Run integration tests as default command
CMD make ci-run
