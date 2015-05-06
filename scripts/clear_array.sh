#!/bin/bash

. ./configure.sh

echo 'Executing clear_array for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q clear_array \
 -w $TILEDB_WORKSPACE/ \
 -A IREG

echo 'Executing clear_array for REG...'
../tiledb_cmd/bin/tiledb_cmd -q clear_array \
 -w $TILEDB_WORKSPACE/ \
 -A REG
