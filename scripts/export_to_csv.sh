#!/bin/bash

echo 'Executing export_to_csv for IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG -f IREG.csv 

echo 'Executing export_to_csv for REG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG -f REG.csv 
