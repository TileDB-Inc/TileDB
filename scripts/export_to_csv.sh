#!/bin/bash

. ./configure.sh

echo 'Executing export_to_csv for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q export_to_csv \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -f IREG.csv 

echo 'Executing export_to_csv for REG...'
../tiledb_cmd/bin/tiledb_cmd -q export_to_csv \
 -w $TILEDB_WORKSPACE/ \
 -A REG \
 -f REG.csv 
