#!/bin/bash

. ./configure.sh

echo 'Executing load_sorted_bin for IREG...'
../tiledb_cmd/bin/tiledb_cmd load_bin \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -f $TILEDB_HOME/data/A.bin

echo 'Executing load_sorted_bin for REG...'
../tiledb_cmd/bin/tiledb_cmd load_bin \
 -w $TILEDB_WORKSPACE/ \
 -A REG \
 -f $TILEDB_HOME/data/A.bin
