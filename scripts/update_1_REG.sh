#!/bin/bash

echo 'Executing update...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q update \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A A_REG -f ~/stavrospapadopoulos/TileDB/data/test_A_1.csv
