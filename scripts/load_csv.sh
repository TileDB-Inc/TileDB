#!/bin/bash

. ./configure.sh

echo 'Executing load_csv for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q load_csv \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -f $TILEDB_HOME/data/test_A_0.csv

echo 'Executing load_csv for REG...'
../tiledb_cmd/bin/tiledb_cmd -q load_csv \
 -w $TILEDB_WORKSPACE/ \
 -A REG \
 -f $TILEDB_HOME/data/test_A_0.csv
