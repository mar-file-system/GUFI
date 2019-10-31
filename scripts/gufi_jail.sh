#!/bin/sh
# Adopted from answer by Celada
# https://security.stackexchange.com/a/118694
#

# Add a section like this to /etc/ssh/sshd_config:
#
# Match Group gufi
#       ForceCommand gufi_jail.sh
#

set -f

set -- ${SSH_ORIGINAL_COMMAND}
case "$1" in
    gufi_find|gufi_ls|gufi_stat|gufi_stats|gufi_query)
        ;;
    *)
        exit 1
esac

command="$1"
shift

exec "${command}" "$@"
