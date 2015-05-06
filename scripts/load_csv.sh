#!/bin/bash

echo 'Executing load_csv for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q load_csv \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A IREG \
 -f ~/stavrospapadopoulos/TileDB/data/test_A_0.csv

echo 'Executing load_csv for REG...'
../tiledb_cmd/bin/tiledb_cmd -q load_csv \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A REG \
 -f ~/stavrospapadopoulos/TileDB/data/test_A_0.csv
