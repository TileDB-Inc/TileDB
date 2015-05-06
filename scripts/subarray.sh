#!/bin/bash

echo 'Executing subarray for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q subarray \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A IREG \
 -R sub_IREG \
 -r 10 -r 20 -r 21 -r 22 

echo 'Executing subarray for REG...'
../tiledb_cmd/bin/tiledb_cmd -q subarray \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A REG \
 -R sub_REG \
 -r 10 -r 20 -r 21 -r 22 

echo 'Exporting subarray result sub_IREG...'
../tiledb_cmd/bin/tiledb_cmd -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A sub_IREG \
 -f sub_IREG.csv 

echo 'Exporting subarray result sub_REG...'
../tiledb_cmd/bin/tiledb_cmd -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A sub_REG \
 -f sub_REG.csv 
