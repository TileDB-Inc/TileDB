#!/bin/bash

. ./configure.sh

echo 'Executing show_array_schema for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q show_array_schema \
 -w $TILEDB_WORKSPACE/ \
 -A IREG 

echo 'Executing load_csv for REG...'
../tiledb_cmd/bin/tiledb_cmd -q show_array_schema \
 -w $TILEDB_WORKSPACE/ \
 -A REG
