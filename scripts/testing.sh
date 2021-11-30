#!/bin/bash

SCRIPTS_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source $SCRIPTS_DIR/build.sh

docker run -it $IMAGE pytest tests
