Key-Value Store
---------------

One of the surprising “side effects” of the TileDB architecture and
design is its ability to model general **key-value stores as sparse 2D
arrays**. The TileDB key-value store inherits all the TileDB array
benefits (fast parallel reads and writes, compression, multiple storage
backend support, multiple language bindings, etc.). Moreover, it offers
a provable **logarithmic retrieval time**, ``O(log T + log c)``, where
``T`` is the number of tiles in the array and ``c`` the tile capacity.

A key-value store essentially stores pairs of the form
``(<key>, <value>)`` and allows for efficient retrieval of ``<value>``
given the corresponding ``<key>``. The TileDB key-value store can be
perceived as a **persistent map** (in the C++ world), or a **persistent
dictionary** (in the Python world). This feature is particularly useful
in applications where the user wishes to attach **arbitrary metadata**
to an array or group.

TileDB models key-value pairs as cells in a sparse array as shown in the
figure below. First, a key-value pair is represented in a more general
form, specifically (i) the key can be of any type and have arbitrary
bitsize (i.e., TileDB supports keys **beyond strings**) and, thus, is
represented as ``<key, key_type, key_size>`` and (ii) the value can
consist of an arbitrary number of partial values, of arbitrary types and
bitsizes, and thus is represented as ``<attr1,attr2, ...>``. TileDB
**hashes** tuple ``<key, key_type, key_size>`` and produces a **16-byte
digest**. Currently, TileDB uses
`MD5 <https://en.wikipedia.org/wiki/MD5>`__ as the hash function, but
this may change in the future. TileDB maps the 16-byte digest into two
8-byte coordinates in a huge domain ``[0, 2^64-1], [0, 2^64-1]``. The
entire key-value pair is stored as a 2D cell at the “digest”
coordinates, with attributes the “value attributes”, plus extra
attributes for ``<key, key_type, key_size`` (so that the key can be
retrieved upon a query as well).

.. _figure-23:

.. figure:: Figure_23.png
    :align: center

    Figure 23: Mapping key-value pairs to 2D sparse array cells

Note that
the array is **super sparse** (for any realistic application). Due to
its ability to store only non-empty cells, the TileDB performance is
unaffected by the domain size. A retrieval of a value given a key is
carried out by a **unary subarray query**; the given key is hashed with
MD5 into coordinates ``(k1, k2)`` and the subarray query becomes
``[k1,k1], [k2,k2]``. TileDB processes unary subarray reads in
logarithmic time in the number of tiles (to retrieve the tile containing
the key-value cell), and logarithmic time in the tile capacity (to
identify the position of the key-value cell in the tile). Note that all
this is done internally; TileDB offers C++ and Python API that behaves
similarly to maps and dictionaries, respectively.

One may wonder **why we chose MD5 as the hash function** or,
alternatively, why we chose 18 bytes as the digest size, when popular
key-value stores utilize very cheap hash functions instead
(significantly faster than MD5). The basic reason is that we want to
**avoid collisions when writing in parallel**. Traditional key-value
stores need to synchronize the parallel processes/threads that write
key-values in the same “bucket” due to a collision of the cheaper hash
function. Handling such collisions would considerably impact the
parallel write performance of TileDB, and substantially make us deviate
from the overall architecture and design of TileDB (which would obviate
the need to build a key-value store with arrays). Moreover, TileDB is
meant to be used for enormous data that need to be stored persistently
(most likely in compressed form). IO and (de)compression can dominate
the hashing cost, which justifies our prioritizing the parallel
read/write performance optimization instead.

.. note::
    We are planning to use a more efficient hash function
    in the near future. We will probably replace MD5 with
    `BLAKE2 <https://blake2.net/>`__ in a subsequent release.
