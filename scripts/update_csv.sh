#!/bin/bash

. ./configure.sh

echo 'Executing update_csv for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q update_csv \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -f $TILEDB_HOME/data/test_A_1.csv

echo 'Executing update_csv for REG...'
../tiledb_cmd/bin/tiledb_cmd -q update_csv \
 -w $TILEDB_WORKSPACE/ \
 -A REG \
 -f $TILEDB_HOME/data/test_A_1.csv
