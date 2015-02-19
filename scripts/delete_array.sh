#!/bin/bash

echo 'Executing delete_array for IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q delete_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG

echo 'Executing delete_array for REG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q delete_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG
