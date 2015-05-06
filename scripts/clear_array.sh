#!/bin/bash

echo 'Executing clear_array for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q clear_array \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A IREG

echo 'Executing clear_array for REG...'
../tiledb_cmd/bin/tiledb_cmd -q clear_array \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A REG
