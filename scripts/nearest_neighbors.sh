#!/bin/bash

echo 'Executing NN for IREG_A...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q nearest_neighbors \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG_A -C 10 -C 10 -N 3 \
 -R NN_IREG_A

echo 'Executing NN for REG_A...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q nearest_neighbors \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG_A -C 10 -C 10 -N 3 \
 -R NN_REG_A

echo 'Exporting subarray result NN_IREG_A...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A NN_IREG_A -f NN_IREG_A.csv 

echo 'Exporting subarray result REG_A...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A NN_REG_A -f NN_REG_A.csv 
