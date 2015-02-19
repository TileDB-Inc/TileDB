#!/bin/bash

echo 'Executing update #2 for IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q update \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG -f ~/stavrospapadopoulos/TileDB/data/test_A_2.csv

echo 'Executing update #2 for REG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q update \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG -f ~/stavrospapadopoulos/TileDB/data/test_A_2.csv
