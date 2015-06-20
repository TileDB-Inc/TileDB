#!/bin/bash

. ./configure.sh

echo 'Executing load_sorted_bin for IREG...'
../tiledb_cmd/bin/tiledb_cmd load_sorted_bin \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -p $TILEDB_HOME/data/A.bin

echo 'Executing load_sorted_bin for REG...'
../tiledb_cmd/bin/tiledb_cmd load_sorted_bin \
 -w $TILEDB_WORKSPACE/ \
 -A REG \
 -p $TILEDB_HOME/data/A.bin
