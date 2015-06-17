#!/bin/bash

. ./configure.sh

echo 'Executing generate_synthetic_data for IREG...'
../tiledb_cmd/bin/tiledb_cmd generate_synthetic_data \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -n 100 \
 -f ../data/A.csv 

