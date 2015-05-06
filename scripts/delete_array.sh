#!/bin/bash

echo 'Executing delete_array for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q delete_array \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A IREG

echo 'Executing delete_array for REG...'
../tiledb_cmd/bin/tiledb_cmd -q delete_array \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A REG
