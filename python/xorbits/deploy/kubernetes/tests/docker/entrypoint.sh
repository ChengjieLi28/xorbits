#!/bin/bash
set -e

cd /opt/xorbits

mkdir -p /mnt/data/.dist-coverage
export COVERAGE_FILE=/mnt/data/.dist-coverage/.coverage

coverage run -m "$1" ${@:2}
