# feature-vector-prototype
Directory for prototyping feature vector / similarity search.  The main program is
called `flat` which does an exhaustive vector-by-vector comparison between a given 
set of vectors and a given set of query vectors. 

## Very basic steps

### Running flat

The `flat` program is a basic program that reads the "texmex" sift feature vector data, either
from a TileDB array or from a file.
The program performs either L2 (default) or (TBD) cosine similarity and checks the result against 
a given ground truth.  If the computed results do not match the ground truth, the
differences are reported on the console.  **Note** A correct computation is not
unique.  That is, multiple vectors may return the same results, i.e., there may
be ties.  Thus, the index values computed by `flat` may differ from the supplied 
ground truth.  You should examine any printed errors on the output for evidence 
of ties.  Most methods in `flat` seem to process ties in the same order as
the ground truth, so this only comes up in one or two cases.

```
flat: feature vector search with flat index.
  Usage:
      tdb (-h | --help)
      tdb (--db_file FILE | --db_uri URI) (--q_file FILE | --q_uri URI) (--g_file FILE | --g_uri URI) 
          [--k NN] [--L2 | --cosine] [--order ORDER] [--hardway] [--nthreads N] [--nqueries N] [--ndb N] [-d | -v]

  Options:
      -h, --help            show this screen
      --db_file FILE        database file with feature vectors
      --db_uri URI          database URI with feature vectors
      --q_file FILE         query file with feature vectors to search for
      --q_uri URI           query URI with feature vectors to search for
      --g_file FILE         ground truth file
      --g_uri URI           ground true URI
      --k NN                number of nearest neighbors to find [default: 10]
      --L2                  use L2 distance (Euclidean)
      --cosine              use cosine distance [default]
      --order ORDER         which ordering to do comparisons [default: gemm]
      --hardway             use hard way to compute distances [default: false]
      --nthreads N          number of threads to use in parallel loops [default: 8]
      --nqueries N          size of queries subset to compare (0 = all) [default: 0]
      --ndb N               size of vectors subset to compare (0 = all) [default: 0]
      -d, --debug           run in debug mode [default: false]
      -v, --verbose         run in verbose mode [default: false]
```


### Build 'flat'
```bash
  cd < project root >
  mkdir build
  cd build
  cmake .. -DCMAKE_BUILD_TYPE=Release -DTileDB_DIR=/path/to/TileDB/cmake
```
**Note:** `flat` builds against `libtiledb`.  You will need to point your `cmake` to the directory that has the cmake configuration files.  That is, if the `libtiledb` you want to use is in `/usr/local/tiledb/lib/libtiledb.so` then you would set `TileDB_DIR` to `/usr/local/tiledb/lib/cmake`
```
  % cmake .. -DTileDB_DIR=/usr/local/tiledb/lib/cmake
```

Note that the following also appears to work.  You can set `TileDB_DIR` to the value of `CMAKE_INSTALL_PREFIX` that was used when building and installing `libtiledb`.  That is, if you built `libtiledb` with
```
  % cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local/tiledb
```
Then if you set `TileDB_DIR` to `/usr/local/tiledb`
```
  % cmake .. -DTileDB_DIR=/usr/local/tiledb
```
the `fetch_content` call in `CMakeLists.txt` will also find the TileDB `cmake` configuration files.
To check if the right path will be searched, look for the output line 
```
-- TileDB include directories are <>
```
This will tell you the path that will be used when building `flat`.  If it isn't the path you are expecting, e.g., if it is the system defaults when you expected something else, check the path you used when invoking `cmake`.

**If you don't specify a value for `TileDB_DIR`** the system default will be used.  That is, you do not have to specify a value for `TileDB_DIR` if the system defaults are good enough.  

`flat` does require a fairly recent version of `libtiledb`.  If you get compilation errors along the lines of 
```
In file included from /home/user/feature-vector-prototype/src/test/unit_sift_array.cpp:5:
/home/user/feature-vector-prototype/src/test/../sift_array.h:67:21: error: expected ';' after expression
    tiledb::Subarray subarray(ctx_, array_);
                    ^
                    ;
/home/user/feature-vector-prototype/src/test/../sift_array.h:67:13: error: no member named 'Subarray' in namespace 'tiledb'
    tiledb::Subarray subarray(ctx_, array_);
    ~~~~~~~~^
/home/user/feature-vector-prototype/src/test/../sift_array.h:68:5: error: use of undeclared identifier 'subarray'
    subarray.set_subarray(subarray_vals);
```
then you likely need a more recent version of `libtiledb`.  To fix this, first try updating your instaleld version of `libtiledb` by invoking the appropriate "upgrade" or "update" command associated with your package manager (if you installed `libtiledb` using a package manager).  Otherwise, obtain an up-to-date version of `libtiledb` from the TileDB github repository at `https://github.com/TileDB-Inc/TileDB` and build and install that per the instructions there.  

**Node:** If you are going to use S3 as a source for TileDB array input, your `libtiledb`
should be built with S3 support.


### About the datasets

`flat` is set up to run with the sift reference arrays available on `http://corpus-texmex.irisa.fr`
and complete info about those arrays can be found there.  The basic characteristics of
the problems are:

| Vector Set  | name      | dimension | nb base       | nb query | nb learn     | format  |
|-------------|-----------|-----------|---------------|----------|--------------|---------|
| ANN_SIFT10K | siftsmall | 128       | 10,000	       | 100	  | 25,000	     | fvecs   |
| ANN_SIFT1M  | sift      | 128	   | 1,000,000	   | 10,000	  | 100,000	     | fvecs   |
| ANN_GIST1M  | gist      | 960       | 1,000,000	   | 1,000	  | 500,000      | 	fvecs  |
|  ANN_SIFT1B | sift1b    | 128	   | 1,000,000,000 | 10,000	  | 100,000,000	 | bvecs   |


These have been ingested as TileDB arrays and can be found at
```text
s3://tiledb-lums/sift/${name}_{base,query,learn,ground_truth}
```
Red permissions are set for all of TileDB.

### Run `flat` with TileDB arrays

Basic invocation of flat requires specifying at least the base set of vectors, the query vectors, and
the ground truth vectors.  Other options 

Example, run with small sized data set
```bash
% ./src/flat --db_uri s3://tiledb-lums/sift/siftsmall_base       \
             --q_uri s3://tiledb-lums/sift/siftsmall_query       \
	     --g_uri s3://tiledb-lums/sift/siftsmall_groundtruth \
             --order gemm
```


Example, run with medium sized data set.
```bash
% ./src/flat --db_uri s3://tiledb-lums/sift/sift_base       \
             --q_uri s3://tiledb-lums/sift/sift_query       \
	     --g_uri s3://tiledb-lums/sift/sift_groundtruth \
             --order gemm
```


Output
```text
# [ Load database, query, and ground truth arrays ]: 11960 ms
# [ Allocating score array ]: 350 ms
# [ L2 comparison (gemm) ]: 1190 ms
# [ L2 comparison colsum ]: 322 ms
# [ L2 comparison outer product ]: 320 ms
# [ L2 comparison finish ]: 140 ms
# [ Get top k ]: 1180 ms
# [ Checking results ]: 0 ms
# [ Total time gemm ]: 4470 ms
```

### Options

The `order` options specify which approach to use for computing the
similarity.  The `qv` and `vq` options compare the feature vectors
to the query vectors one by one.  The `qv` order has queries on
the outer loop and feature vectors on the inner loop.  The `vq`
oder has vectors on the outer loop and queries on the inner loop.
The `gemm` order uses the optimized linear algebra matrix-matrix
produce algorithm to do the comparisons.  (Presentation of details
of this TBD.)

`flat` is parallelized using `std::async`.  The `--nthreads` option
specifies how many threads to use.  In environments where `gemm` is
provided by MKL, the `OMP_NUM_THREADS` or `MKL_NUM_THREADS` option
will set the number of threads to be used by `gemm`.  The default
in both cases is the number of available hardware threads.  **Note:**
This problem is largely memory-bound.  Threading will help, but
you should not expect linear speedup.

The `--nqueries` option specifies how many of the query vectors to
use.  The default is to use all of them.  Setting this to a
smaller number can be useful for testing and will also allow
the medium dataset to fit into memory of a desktop machine.

The `--ndb` option specifies how much of the data set to
use.  Note that when using this that the computed result will
almost surely differ from the ground truth because all
potential neighbors have not been considered.  This may be more 
useful as a performance testing mechanism (e.g.) but we
would need to silence error reporting (perhaps simply by
not supplying a ground truth file/array).

The `--k` option specifies how many neighbors to keep (i.e.,
for "top k").

The `--hardway` option specifies how to collect the top k
data.  If this option is set, A complete set of scores is
computed and then filtered for the top k vectors.  If 
the value is not set, a heap is used to filter the scores
on the fly.  This only affects the `qv` and `vq` options.
The `gemm` option computes all scores.

The `-v` and `-d` options turn on verbosity and debugging,
respectively.  (There is not currently a large amount
of output for this.)

### Observations

The `gemm` approach appears to be by far the fastest, and is
about a factor of four faster than the open source `faiss` 
project for the flat L2 query.

For the other approaches, different parameter values may result
in different performance.  Significant experimentation would need
to be done to find those, however, and it isn't clear that the
performance of `gemm` could be matched at any rate. 


### Get file data (optional)
```bash
  cd < project root >
  mkdir -p external/data
  cd external/data
  wget ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz
  tar zxf siftsmall.tar.gz
  wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
  tar zxf sift.tar.gz
```

### Ingesting the data
The python notebook `python/ingest_eq.ipynb` was used to
convert the original data files to TileDB arrays in S3.  **Important:**
Do not re-run this unless you change the paths to avoid
overwriting the existing arrays.


### Run `flat` with local files

`flat` can also be run with the original files from the data repository.
```bash
  cd < project root >
  cd build

  # Get help
  ./src/flat -h

  # Run with small data
  ./src/flat  --db_file ../external/data/siftsmall/siftsmall_base.fvecs \
              --q_file ../external/data/siftsmall/siftsmall_query.fvecs \
              --g_file ../external/data/siftsmall/siftsmall_groundtruth.ivecs \
              --k 100
```
This specifies to run `flat` on the `siftsmall` data, checking the first 100 neighbors.  If all of the checks pass, the program will simply return 0.  Otherwise, it will print error messages for each of its computed neighbors that does not pass (does not match ground truth).

You can also run on the medium data set:
```c++
  # Run with small data
  time ./src/flat  --db_file ../external/data/sift/sift_base.fvecs \
                    --q_file ../external/data/sift/sift_query.fvecs \
                    --g_file ../external/data/sift/sift_groundtruth.ivecs \
                    --k 100
```

The memory and CPU requirements for the 1B dataset become prohibitive and probably can't be run on a typical desktop or typical server.

## Main TODO items

(**Note:** We can now say that TileDB can be used for similarity search.)

* **Use OpenBLAS instead of MKL for gemm** and incorporate into the build process.  This should take less than a day.
* **Move prototype into core as a query.** If I can work with Luc this shouldn't take more than a week (and would not take up anywhere close to a week for Luc).  This shouldn't take more than a week, depending on how fancy we want to be.  Probably need to add a day or two to deal with designing the API.
* **Provide --id argument** to allow selection of a single vector to query
* **Use streaming approach to handle arrays/files that won't fit in local memory of one machine.**  This could be done before moving into core or afterwords.  It is probably better to do this with the prototype.  This shouldn't take more than two days.
* **Use parallel/distributed approach** to handle arrays/files that won't fit in local memory of one machine.  This is doable if we want to just parallelize an application using libtiledb.  However, if we want to do the similarity search "in the cloud" it might be better to orchestrate the distributed computation at the Python task graph level.
* **Improve performance with better blocking for memory use** In conjunction with reorganizing for out-of-core operation, we should also arrange the in-memory computation to make better use of the memory hierarchy.
* **Finish implementation of cosine similarity** This should be fairly straightforward to implement. However, we have not ground truth to verify it with the sift benchmark dataset.
* **Make ground truth comparison an optional argument**
* **Perform ground truth comparison only when requested**
* **Implement parallelism using task graph** This should not be too difficult, a couple of days, as the `std::async` parallelism is similar to very basic task graph usage.
