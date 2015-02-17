#!/bin/bash

echo 'Executing load...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q load \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A A_REG -f ~/stavrospapadopoulos/TileDB/data/test_A_0.csv
