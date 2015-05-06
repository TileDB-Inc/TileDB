#!/bin/bash

echo 'Executing update_csv for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q update_csv \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A IREG \
 -f ~/stavrospapadopoulos/TileDB/data/test_A_1.csv

echo 'Executing update_csv for REG...'
../tiledb_cmd/bin/tiledb_cmd -q update_csv \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A REG \
 -f ~/stavrospapadopoulos/TileDB/data/test_A_1.csv
