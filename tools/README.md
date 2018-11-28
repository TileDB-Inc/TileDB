# TileDB CLI tools

This directory contains the source for the TileDB CLI.

**Note**: These tools should currently be considered experimental, and they are not built by default with TileDB.

## Building

To build the `tiledb` CLI executable, first configure TileDB with the CMake variable `-DTILEDB_TOOLS=ON`. Then use the `tools` make target e.g.

```bash
$ cmake -DTILEDB_TOOLS=ON ..
$ make && make -C tiledb tools
```

This will produce the binary `tiledb/tools/tiledb`.

## Running

To display usage information, simply execute the `tiledb` program:

```bash
$ tiledb
Command-line interface for performing common TileDB tasks. Choose a command:

    tiledb help <command>
    tiledb info --array <uri> [--tile-sizes]
```

To display help about a particular command, use `tiledb help <command>`, e.g.:

```bash
$ tiledb help info
DESCRIPTION
Displays information about a TileDB array.

SYNOPSIS
    tiledb info --array <uri> [--tile-sizes]

OPTIONS
    --array <uri>   URI of TileDB array
    --tile-sizes    Print statistics about tile sizes in the array.
```