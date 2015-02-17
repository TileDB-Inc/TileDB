#!/bin/bash

echo 'Executing load...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q load \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A A -f ~/stavrospapadopoulos/TileDB/data/test_A_0.csv
