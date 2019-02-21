#/usr/bin/env bash

set -e

function ppde {
    docker exec "${TRAVIS_JOB_NUMBER}" "$@"
}

docker container stop $(docker container ls -aq) || true
docker container rm   $(docker container ls -aq) || true

# get the image
docker pull "${DOCKER_IMAGE}"

# start container
docker run -it -d --name "${TRAVIS_JOB_NUMBER}" -v $(realpath .):/GUFI -w /GUFI "${DOCKER_IMAGE}" bash
