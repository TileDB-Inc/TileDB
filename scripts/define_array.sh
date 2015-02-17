#!/bin/bash

# Array schema for A
echo 'Executing define_array...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q define_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A A -o hilbert -a attr1 -a attr2 -d i -d j \
 -D 0 -D 100 -D 0 -D 100 \
 -t float -t int -t int64_t
