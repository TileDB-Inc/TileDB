#!/bin/bash

echo 'Executing retile for IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q retile \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG -c 100

