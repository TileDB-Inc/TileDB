#!/bin/bash

echo 'Executing export_to_csv for IREG...'
../tiledb_cmd/bin/tiledb_cmd -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A IREG \
 -f IREG.csv 

echo 'Executing export_to_csv for REG...'
../tiledb_cmd/bin/tiledb_cmd -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/example/ \
 -A REG \
 -f REG.csv 
