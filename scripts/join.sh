#!/bin/bash

echo 'Executing define_array for IREG_A...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q define_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG_A -o hilbert -a attr1 -a attr2 -d i -d j \
 -D 0 -D 100 -D 0 -D 100 \
 -t float -t int -t int64_t

echo 'Executing define_array for REG_A...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q define_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG_A -o hilbert -a attr1 -a attr2 -d i -d j \
 -D 0 -D 100 -D 0 -D 100 \
 -t float -t int -t int64_t \
 -e 5 -e 5

echo 'Executing define_array for IREG_B...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q define_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG_B -o hilbert -a attr1 -a attr2 -a attr3 -d i -d j \
 -D 0 -D 100 -D 0 -D 100 \
 -t float -t int -t int -t int64_t

echo 'Executing define_array for REG_B...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q define_array \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG_B -o hilbert -a attr1 -a attr2 -a attr3 -d i -d j \
 -D 0 -D 100 -D 0 -D 100 \
 -t float -t int -t int -t int64_t \
 -e 5 -e 5

echo 'Executing load for IREG_A...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q load \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG_A -f ~/stavrospapadopoulos/TileDB/data/test_A_0.csv

echo 'Executing load for IREG_B...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q load \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG_B -f ~/stavrospapadopoulos/TileDB/data/test_B.csv

echo 'Executing load for REG_A...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q load \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG_A -f ~/stavrospapadopoulos/TileDB/data/test_A_0.csv

echo 'Executing load for REG_B...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q load \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG_B -f ~/stavrospapadopoulos/TileDB/data/test_B.csv

echo 'Executing join between IREG_A and IREG_B'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q join \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG_A -A IREG_B -R IREG_C 

echo 'Executing join between REG_A and REG_B'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q join \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG_A -A REG_B -R REG_C 

echo 'Executing export_to_csv for IREG_C...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG_C -f IREG_C.csv 

echo 'Executing export_to_csv for REG_C...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q export_to_csv \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG_C -f REG_C.csv 
