#/usr/bin/env bash

set -e

# get the image
docker pull "${DOCKER_IMAGE}"

# start container
docker run -it -d --name "${TRAVIS_JOB_NUMBER}" -v $(realpath .):/GUFI -w /GUFI "${DOCKER_IMAGE}" bash

# quick function to shorten running of commands in docker
function de {
    docker exec "${TRAVIS_JOB_NUMBER}" "$@"
}
