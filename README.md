TileDB
======

The TileDB array database system

Examples
========

To run example scripts, do the following: <br />
  $ cd scripts
  
  Use your favourite editor to create a workspace directory and
  update TILEDB_WORKSPACE and TILEDB_HOME environment variables
  in the configure.sh script

  $ ./define_array.sh (defines the schema of two arrays) <br /> 
  $ ./load_array.sh (loads the above raw csv file into the two arrays)  <br />
  $ ./export_to_csv.sh (exports the contents of the two arrays back to a csv file)  <br />
  $ more REG.csv  <br />
  $ more IREG.csv

