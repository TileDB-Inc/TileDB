#!/bin/bash

echo 'Executing subarray for IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q subarray \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG -R sub_IREG \
 -r 10 -r 20 -r 21 -r 22 

echo 'Executing subarray for REG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q subarray \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG -R sub_REG \
 -r 10 -r 20 -r 21 -r 22 

echo 'Exporting subarray result sub_IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A sub_IREG -f sub_IREG.csv 

echo 'Exporting subarray result sub_REG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A sub_REG -f sub_REG.csv 
