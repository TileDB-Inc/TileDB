#!/bin/bash

. ./configure.sh

# Array schema for IREG
echo 'Executing define_array for IREG...'
../tiledb_cmd/bin/tiledb_cmd define_array \
 -w $TILEDB_WORKSPACE/ \
 -A IREG \
 -a 'a1,a2' \
 -d 'i,j' \
 -D '0,100,0,100' \
 -t 'float:var,char:3,int64_t' \
 -o column-major \
 -c 6 \
 -s 1 

# Array schema for REG
echo 'Executing define_array for REG...'
../tiledb_cmd/bin/tiledb_cmd define_array \
 -w $TILEDB_WORKSPACE/ \
 -A REG \
 -a 'a1,a2' \
 -d 'i,j' \
 -D '0,100,0,100' \
 -e '5,5' \
 -t 'float:var,char:3,int64_t' \
 -o hilbert \
 -O row-major \
 -s 1
