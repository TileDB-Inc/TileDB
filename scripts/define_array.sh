#!/bin/bash

# Array schema for IREG
echo 'Executing define_array for IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q define_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG -o hilbert -a attr1 -a attr2 -d i -d j \
 -D 0 -D 100 -D 0 -D 100 \
 -t float -t int -t int64_t -c 6

# Array schema for REG
echo 'Executing define_array for REG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q define_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG -o hilbert -a attr1 -a attr2 -d i -d j \
 -D 0 -D 100 -D 0 -D 100 \
 -t float -t int -t int64_t \
 -e 5 -e 5
