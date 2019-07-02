#!/usr/bin/env bash
# Source this file to set up fake environment variables
# when running Travis CI scripts locally.

export C_COMPILER=gcc-8
export CXX_COMPILER=g++-8
export TRAVIS_JOB_NUMBER=${RANDOM}
export BUILD=cmake
export DOCKER_IMAGE=centos:7

# cleanup old container with the same name
docker container stop "${TRAVIS_JOB_NUMBER}" || true
docker container rm   "${TRAVIS_JOB_NUMBER}" || true
