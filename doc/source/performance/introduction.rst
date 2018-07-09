.. _performance/introduction:

Introduction to Performance
===========================

TileDB is a powerful array storage manager that provides you with great
flexibility to adapt it to your application. If tuned properly,
TileDB can offer extreme performance even in the most challenging settings.
Unfortunately, performance tuning in general is a complex topic, and there
are many factors that influence the performance of TileDB. There is no unified
way to improve performance for all TileDB programs, as the space of
tradeoffs is quite large, and each use case can potentially benefit from
very different choices for these tradeoffs.

To facilitate performance tuning, we provide two very helpful tutorials.
In :ref:`using-tiledb-statistics`, we show how to enable recording
statistics when submitting your write/read queries, and how to print
and interpret them. Reading the statistics, we explain through examples how
to identify areas for optimization and demonstrate that tuning the TileDB
parameters can lead to better performance. In :ref:`performance-factors`, we
summarize and explain all the factors that affect the TileDB performance.

We are always happy to help in case you do not get the desired
performance out of TileDB in your use case. Please post a comment on
the `TileDB Forum <https://forum.tiledb.io/>`_ or drop us a line at
``hello@tiledb.io`` sharing with us the way you create/write/read
your TileDB arrays and we will quickly get back to you with suggestions.

