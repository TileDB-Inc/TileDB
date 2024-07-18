# TileDB CLI tools

This directory contains the source for the TileDB CLI.

**Note**: These tools should currently be considered experimental, and they are not built by default with TileDB.

## Building

To build the `tiledb` CLI executable, first configure TileDB with the CMake variable `-DTILEDB_TOOLS=ON`. Then use the `tools` make target e.g.

```bash
$ cmake -DTILEDB_TOOLS=ON ..
$ make tools
```

This will produce the binary `tiledb/tools/tiledb`.

## Running

To display usage information, simply execute the `tiledb` program:

```bash
$ tiledb
Command-line interface for performing common TileDB tasks. Choose a command:

    tiledb help <command>
    tiledb info array-schema -a <uri>
    tiledb info tile-sizes -a <uri>
    tiledb info dump-mbrs -a <uri> [-o <path>]
    tiledb info svg-mbrs -a <uri> [-o <path>] [-w <N>] [-h <N>]
```

To display help about a particular command, use `tiledb help <command>`, e.g.:

```bash
$ tiledb help info
DESCRIPTION
Displays information about a TileDB array.

SYNOPSIS
    tiledb info array-schema -a <uri>
    tiledb info tile-sizes -a <uri>
    tiledb info dump-mbrs -a <uri> [-o <path>]
    tiledb info svg-mbrs -a <uri> [-o <path>] [-w <N>] [-h <N>]

OPTIONS
    array-schema: Prints basic information about the array's schema.
        -a, --array <uri>     URI of TileDB array

    tile-sizes: Prints statistics about tile sizes in the array.
        -a, --array <uri>     URI of TileDB array

    dump-mbrs: Dumps the MBRs in the array to text output.
        -a, --array <uri>     URI of TileDB array
        -o, --output          Path to write output text file

    svg-mbrs: Produces an SVG visualizing the MBRs (2D arrays only)
        -a, --array <uri>     URI of TileDB array
        -o, --output          Path to write output SVG
        -w, --width           Width of output SVG
        -h, --height          Height of output SVG
```