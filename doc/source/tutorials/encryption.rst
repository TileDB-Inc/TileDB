.. _encryption:

Encryption
==========

In this tutorial you will how to use the at-rest encryption feature for TileDB arrays. It is
recommended that you read the dense arrays tutorial first.

.. table:: Full programs
  :widths: auto

  ====================================  =============================================================
  **Program**                           **Links**
  ------------------------------------  -------------------------------------------------------------
  ``encryption``                        |encryptioncpp|
  ====================================  =============================================================

.. |encryptioncpp| image:: ../figures/cpp.png
   :align: middle
   :width: 30
   :target: {tiledb_src_root_url}/examples/cpp_api/encryption.cc


Basic concepts and definitions
------------------------------

.. toggle-header::
    :header: **At-rest encryption**

    TileDB allows you to configure arrays such that all attribute data and array
    metadata is encrypted before being persisted. When reading data from
    encrypted arrays, the data and metadata are unencrypted in main memory.
    This is known as **at-rest** encryption.


Creating an encrypted array
---------------------------

Creating an encrypted array is very similar to the unencrypted case. First,
configure the array schema with a domain, attributes, etc. in the usual way:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Context ctx;
        // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
        Domain domain(ctx);
        domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
              .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));

        ArraySchema schema(ctx, TILEDB_DENSE);
        schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
        schema.add_attribute(Attribute::create<int>(ctx, "a"));

Next, create the array. Note that when creating an encrypted array, you must
specify the encryption algorithm and the key to use:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        // Load the encryption key from disk, environment variable, etc.
        // Here we use a string for convenience.
        const char encryption_key[32 + 1] = "0123456789abcdeF0123456789abcdeF";

        // Create the encrypted array.
        Array::create(array_name, schema,
            TILEDB_AES_256_GCM, encryption_key, strlen(encryption_key));

The encryption key must be provided when the array is created because TileDB
also encrypts metadata such as the array schema. This same encryption algorithm
and key will need to be provided again for any further array operations such as
reading and writing.


Writing to an encrypted array
-----------------------------

Writing to an encrypted array is also very similar to the unencrypted case. The
only difference is that when opening the array, you must specify the same
encryption algorithm and key as you used when creating the array.

To start, prepare the **unencrypted** (plaintext) data to be written:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        std::vector<int> data = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      Next, open the array for writing. Note that now we must also specify the
      encryption algorithm and correct encryption key when opening the array.

      .. code-block:: c++

        Context ctx;
        Array array(ctx, array_name, TILEDB_WRITE,
            TILEDB_AES_256_GCM, encryption_key, strlen(encryption_key));
        Query query(ctx, array);

      Then, set up the query as normal, submit it, and close the array.

      .. code-block:: c++

        query.set_layout(TILEDB_ROW_MAJOR).set_buffer("a", data);
        query.submit();
        array.close();

The data for attribute ``a`` is now stored **encrypted** on disk.

If you specify an incorrect or invalid encryption key when opening the array,
TileDB will return an error, meaning the array was not opened and therefore
cannot be written to.


Reading from an encrypted array
-------------------------------

As with writing, the only difference when reading from encrypted arrays is that
you must open the array with the correct encryption key.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      First, open the array for reading, specifying the same encryption key:

      .. code-block:: c++

        Context ctx;
        Array array(ctx, array_name, TILEDB_READ,
            TILEDB_AES_256_GCM, encryption_key, strlen(encryption_key));

      Then, we set up and submit a query object, and close the array, all as
      usual.

      .. code-block:: c++

        const std::vector<int> subarray = {1, 2, 2, 4};
        std::vector<int> data(6);

        Query query(ctx, array);
        query.set_subarray(subarray)
             .set_layout(TILEDB_ROW_MAJOR)
             .set_buffer("a", data);
        query.submit();
        array.close();

Now ``data`` holds the **decrypted** (plaintext) result values from attribute
``a``.

If you specify an incorrect or invalid encryption key when opening the array,
TileDB will return an error, meaning the array was not opened and therefore
cannot be read from.

.. note::

    By default, TileDB caches array data and metadata in main memory after
    opening and reading from arrays. These caches will store decrypted
    (plaintext) array data in the case of encrypted arrays. For a bit of extra
    in-flight security (at the cost of performance), you can disable the TileDB
    caches. See the tutorial on :ref:`config` to learn how to do that.


Supported encryption algorithms
-------------------------------

TileDB currently supports a single type of encryption, AES-256 in the GCM mode,
which is a symmetric, authenticated encryption algorithm. The details of this
encryption method are outside the scope of this tutorial, but at a high level
this means when creating, reading or writing arrays you must provide the same
256-bit encryption key. The authenticated nature of the encryption scheme means
that a message authentication code (MAC) is stored together with the encrypted
data, allowing verification that the persisted ciphertext was not modified.

On macOS and Linux TileDB uses the `OpenSSL <https://www.openssl.org>`__ library
for encryption, and the
`next generation cryptography (CNG) <https://docs.microsoft.com/en-us/windows/desktop/seccng/cng-portal>`__
API on Windows.


Encryption key lifetime
-----------------------

TileDB never persists the encryption key, but TileDB does store a copy of the
encryption key in main memory while an encrypted array is open. When the array
is closed, TileDB will zero out the memory used to store its copy of the key,
and free the associated memory.


Performance
-----------

Due to the extra processing required to encrypt and decrypt array metadata and
attribute data, you may experience lower performance on opening, reading and
writing for encrypted arrays.

To mitigate this, TileDB internally parallelizes encryption and decryption using
a chunking strategy. Additionally, when compression or other filtering is
configured on array metadata or attribute data, encryption occurs last,
meaning the compressed (or filtered in general) is what gets encrypted.

Finally, newer generations of some Intel and AMD processors offer instructions
for hardware acceleration of encryption and decryption. The encryption libraries
that TileDB employs are configured to use hardware acceleration if it is
available.