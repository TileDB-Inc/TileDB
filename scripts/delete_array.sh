#!/bin/bash

. ./configure.sh

echo 'Executing delete_array for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q delete_array \
 -w $TILEDB_WORKSPACE/ \
 -A IREG

echo 'Executing delete_array for REG...'
../tiledb_cmd/bin/tiledb_cmd -q delete_array \
 -w $TILEDB_WORKSPACE/ \
 -A REG
