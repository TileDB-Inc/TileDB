
TileDB
======

The TileDB array database system

Examples
========

To run example scripts, do the following:

    1. Create a workspace directory
    2. Update TILEDB_WORKSPACE and TILEDB_HOME environment variables
    3. $ cd scripts 
  
    $ ./define_array.sh (defines the schema of two arrays)
    $ ./load_array.sh (loads the above raw csv file into the two arrays)
    $ ./export_to_csv.sh (exports the contents of the two arrays back to a csv file)
    $ more REG.csv
    $ more IREG.csv

TroubleShooting
===============

* C++11 compile error: If the C++11 option is not recognized, try the following:
  * Upgrade GCC. The version must be 4.3 or higher
  * Use -std=c++0x instead
  * If OS is CentOS or RedHat, use the CXX = mpiicpc instead of mpicxx
  * Download latest Intel C/C++ compiler
