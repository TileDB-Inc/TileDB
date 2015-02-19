#!/bin/bash

echo 'Executing clear_array for IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q clear_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG

echo 'Executing clear_array for REG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q clear_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG
