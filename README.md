Introduction
============

TileDB is a new array storage manager, particularly efficient for the case of
**sparse** arrays. It currently offers a small set of queries implemented
as stand-alone Linux programs. It also offers a high level C API that allows various
programming languages to manage persistent arrays by interfacing with TileDB as a library.
TileDB is envisioned to be used either as an HDF5-like distributed file system,
or as a full-fledged distributed array database.

TileDB is currently under heavy development. Benchmark results, tutorials,
and research papers are under way. Stay tuned!

Kick-start for users
====================

Run the following commands to get started, `packages.txt` contains some
packages required to build the documentation, you can replace `apt-get` with
your package manager of choice.

```bash
git clone git@github.com:stavrospapadopoulos/TileDB.git
cd TileDB
cat packages.txt | xargs sudo apt-get -y install
make; make doc;
```
The TileDB executable programs can be found in directory `tiledb_cmd/bin`, and
their corresponding documentation is in directory `manpages`, where you can
find either the man files inside subdirectory `manpages/man` or the respective
html in `manpages/html`.

Perform the following steps to get familiar with the TileDB command line
interface:

1. Define an array of your choice with `tiledb_define_array`. Read the
   documentation, it is quite extensive and it provides examples.

2. Generate a synthetic dataset for the array you defined using
   `tiledb_generate_data`, always reading the documentation. Try a CSV file
   first, so that you can see how it looks like in a text editor.

3. Load the dataset into your array using `tiledb_load_csv`. Check what kind
   of files TileDB creates in the workspace folder you provided. Every array is
   essentially a folder of binary files with simple metadata, similar to HDF5.

4. Now you can do other various things, such as export the dataset back to a
   CSV file, choose a subarray, and make an update. Play with the queries,
   consulting their documentation.

Kick-start for developers
=========================

Everything you need is in `core/include/capis/`. Given that you have run
`make doc`, you can see the documentation of each C API with detailed 
guidelines on how to use it. Contact me if you require any particular 
C API and I will try to develop it in a short time.

Under construction
==================

I will spend some time stabilizing the current code, heavily testing and
documenting it. At the same time, I have a list with C APIs I gathered 
from you, so I will try to have everything ready soon. Finally, we are
preparing a paper with the specifics of the storage manager, as well
as tutorials for both users and developers.

TroubleShooting
===============

* C++11 compile error: If the C++11 option is not recognized, try the following:
  * Upgrade GCC. The version must be 4.3 or higher
  * Use `-std=c++0x` instead
  * If OS is CentOS or RedHat, use the `CXX = mpiicpc` instead of `mpicxx`
  * Download latest Intel C/C++ compiler
* MPI-3 support is required, so make sure that the default MPI implements v3 of 
the standard.
