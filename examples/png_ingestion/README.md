# TileDB example: PNG image data

This directory contains the example program from the [PNG ingestion and slicing tutorial](https://docs.tiledb.io/en/latest/real-world-examples/dense-image-data.html).

## Build

Required dependencies: TileDB and [libpng](https://sourceforge.net/projects/libpng/).

```bash
$ mkdir build
$ cd build
$ cmake .. && make
```

This creates the executable `tiledb_png`.

## Run

Ingests `input.png` into a new array `my_array_name`, slices and produces a new output image `output.png`:

```bash
./tiledb_png input.png my_array_name output.png
```