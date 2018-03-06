Writing
=======

Each **write session** writes cells sequentially in batches, creating a
separate fragment. A write session begins when a query is created in
write mode, comprises one or more query submissions, and terminates when
the query is freed (this will become clearer soon). Depending on the way
the query is created, the new fragment may be dense or sparse. Recall
that a dense array may be comprised of dense or sparse fragments, while
a sparse array can only be comprised of sparse fragments. We explain
writing to a dense and sparse array separately.

.. toctree::
    :maxdepth: 2

    writing-dense-data
    writing-sparse-data