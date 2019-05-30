.. _datetimes:

Datetimes
=========

TileDB supports datetime values for attributes and array domains. The representation
used by TileDB follows the design of Numpy's ``np.datetime64``
`datatype <https://docs.scipy.org/doc/numpy/reference/arrays.datetime.html>`__.

Representation
--------------

Values for the ``TILEDB_DATETIME_*`` types are internally stored and
manipulated as ``int64_t`` values. From the perspective of TileDB internally,
the datetime datatypes are simply aliases for ``TILEDB_INT64``.

The meaning of a integral datetime value depends on three things:

1. A reference date. TileDB fixes this to the UNIX epoch time (1970-01-01 at
   12:00 am). This is not currently configurable.
2. A unit of time. For example: day, month, hour, or nanosecond.
3. An integer value. This the integer number of time units relative to the
   reference date.

For example, a value of ``10`` for the type ``TILEDB_DATETIME_DAY`` refers to
12:00 am on 1970-01-10. A value of ``-18`` for the type ``TILEDB_DATETIME_HR``
refers to 6:00 am on 1969-12-31, or ``1969-12-31T06:00Z`` in ISO8601 format.

This means that each date unit of datetime is capable of representing a different
range of dates at different resolutions. The following table (values taken from
the
`Numpy np.datetime64 documentation <https://docs.scipy.org/doc/numpy/reference/arrays.datetime.html#datetime-units>`__)
summarizes each date unit's relative and absolute range:

==========================   ====================    ======================
Datatype                     Time span (relative)    Time span (absolute)
==========================   ====================    ======================
``TILEDB_DATETIME_YEAR``     +/- 9.2e18 years        [9.2e18 BC, 9.2e18 AD]
``TILEDB_DATETIME_MONTH``    +/- 7.6e17 years        [7.6e17 BC, 7.6e17 AD]
``TILEDB_DATETIME_WEEK``     +/- 1.7e17 years        [1.7e17 BC, 1.7e17 AD]
``TILEDB_DATETIME_DAY``      +/- 2.5e16 years        [2.5e16 BC, 2.5e16 AD]
``TILEDB_DATETIME_HR``       +/- 1.0e15 years        [1.0e15 BC, 1.0e15 AD]
``TILEDB_DATETIME_MIN``      +/- 1.7e13 years        [1.7e13 BC, 1.7e13 AD]
``TILEDB_DATETIME_SEC``      +/- 2.9e11 years        [2.9e11 BC, 2.9e11 AD]
``TILEDB_DATETIME_MS``       +/- 2.9e8 years         [2.9e8 BC, 2.9e8 AD]
``TILEDB_DATETIME_US``       +/- 2.9e5 years         [290301 BC, 294241 AD]
``TILEDB_DATETIME_NS``       +/- 292 years           [1678 AD, 2262 AD]
``TILEDB_DATETIME_PS``       +/- 106 days            [1969 AD, 1970 AD]
``TILEDB_DATETIME_FS``       +/- 2.6 hours           [1969 AD, 1970 AD]
``TILEDB_DATETIME_AS``       +/- 9.2 seconds         [1969 AD, 1970 AD]
==========================   ====================    ======================


Interfacing with Numpy
----------------------

Datetime support in core TileDB depends on the high-level APIs for each language
to provide usability and interoperability with high-level datetime objects.
The rest of this tutorial will demonstrate using the TileDB Python API,
where ``np.datetime64`` objects can be used directly.

The following example shows how to use ``np.datetime64`` to create a 1D dense
TileDB array with a domain of 10 years, at the day resolution.

.. content-tabs::

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         import numpy as np

         # Domain is 10 years, day resolution, one tile per 365 days
         dim = tiledb.Dim(name='d1', ctx=ctx,
                          domain=(np.datetime64('2010-01-01'), np.datetime64('2020-01-01')),
                          tile=np.timedelta64(365, 'D'),
                          dtype=np.datetime64('', 'D').dtype)
         dom = tiledb.Domain(dim, ctx=ctx)
         schema = tiledb.ArraySchema(ctx=ctx, domain=dom,
                                     attrs=(tiledb.Attr('a1', dtype=np.float64),))

         tiledb.Array.create(my_array_name, schema)

Note the usage of ``np.timedelta64`` to specify the tile extent of the vector.

Writing a range of values to the array is as expected:

.. content-tabs::

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         # Randomly generate 2 years of values for attribute 'a1'
         ndays = 365 * 2
         a1_vals = np.random.rand(ndays)

         # Write the data at the beginning of the domain.
         start = np.datetime64('2010-01-01')
         end = start + np.timedelta64(ndays - 1, 'D')

         with tiledb.DenseArray('my_array_name', 'w', ctx=ctx) as T:
             T[start: end] = {'a1': a1_vals}

The array can also be sliced using datetimes when reading:

.. content-tabs::

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         # Slice a few days from the middle using two datetimes
         with tiledb.DenseArray('my_array_name', 'r', attr='a1', ctx=ctx) as T:
             vals = T[np.datetime64('2010-11-01'): np.datetime64('2011-01-31')]

Because internally datetimes are ``int64_t`` values, slicing datetime
dimensions in this case is just as efficient as other domain types.