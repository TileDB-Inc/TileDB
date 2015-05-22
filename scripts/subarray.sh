#!/bin/bash

. ./configure.sh

echo 'Executing subarray for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q subarray \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -R sub_IREG \
 -r '10,20,21,22' 

echo 'Executing subarray for REG...'
../tiledb_cmd/bin/tiledb_cmd -q subarray \
 -w $TILEDB_WORKSPACE/ \
 -A REG \
 -R sub_REG \
 -r '10,20,21,22' 

echo 'Exporting subarray result sub_IREG...'
../tiledb_cmd/bin/tiledb_cmd -q export_to_csv \
 -w $TILEDB_WORKSPACE/ \
 -A sub_IREG \
 -f sub_IREG.csv 

echo 'Exporting subarray result sub_REG...'
../tiledb_cmd/bin/tiledb_cmd -q export_to_csv \
 -w $TILEDB_WORKSPACE/ \
 -A sub_REG \
 -f sub_REG.csv 
