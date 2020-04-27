#!/usr/bin/env bash
# This script installs python-cryptography,
# python-bcrypt, and python-pynacl with pip.

set -e

# check if running as root
USER=""
if [[ "$(id -u)" -ne 0 ]]
then
    USER="--user"
fi

pip3 install ${USER} cryptography bcrypt pynacl
