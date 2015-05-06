#!/bin/bash

echo 'Executing show_array_schema for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q show_array_schema \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A IREG 

echo 'Executing load_csv for REG...'
../tiledb_cmd/bin/tiledb_cmd -q show_array_schema \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A REG
