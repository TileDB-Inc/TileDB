#!/bin/bash

. ./configure.sh

echo 'Executing generate_synthetic_data to produce CSV file for IREG...'
../tiledb_cmd/bin/tiledb_cmd generate_synthetic_data \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -n 100 \
 -s 0 \
 -f ../data/A.csv \
 -t csv

echo 'Executing generate_synthetic_data to produce BIN file for IREG...'
../tiledb_cmd/bin/tiledb_cmd generate_synthetic_data \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -n 100 \
 -s 0 \
 -f ../data/A.bin \
 -t bin

