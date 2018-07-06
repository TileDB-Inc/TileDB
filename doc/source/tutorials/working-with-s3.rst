Working with S3
===============

This is a simple guide that demonstrates how to use TileDB on S3-compatible
backends. We will first explain how to set up an AWS account and configure
TileDB for `AWS S3 <https://aws.amazon.com/s3/>`_, and then show how to
set up and use `minio <https://minio.io>`_ locally.

After setting up TileDB to work with AWS S3 or minio, your TileDB programs
will function properly without any API change! All you need to
do is, instead of using local file system paths when creating/accessing
groups, arrays, maps, and VFS files, use URIs that start with ``s3://``.
For instance, if you wish to create (and subsequently write/read) an
array on S3, you use URI ``s3://<your-bucket>/<your-array-name>``
for the array name.

AWS
---

First, we need to set up an AWS account and generate access keys.

1. Create a new `AWS account <https://portal.aws.amazon.com/billing/signup#/start>`_.

2. Visit the `AWS console <https://aws.amazon.com/console/>`_ and sign in.

3. On the AWS console, click on the ``Services`` drop-down menu and select ``Storage->S3``. You can create S3 buckets there.

4. On the AWS console, click on the ``Services`` drop-down menu and select ``Security, Identity & Compliance->IAM``.

5. Click on ``Users`` from the left-hand side menu, and then click on the ``Add User`` button. Provide the email or username of the user you wish to add, select the ``Programmatic Access`` checkbox and click on ``Next: Permissions``.

6. Click on the ``Attach existing policies directly`` button, search for the S3-related policies and add the policy of your choice (e.g., full-access, read-only, etc.). Click on ``Next`` and then ``Create User``.

7. Upon successful creation, the next page will show the user along with two keys: ``Access key ID`` and ``Secret access key``. Write down both these keys.

8. Export these keys to your environment from a console:

.. content-tabs::

   .. tab-container:: linux-macos
      :title: Linux / macOS

      .. code-block:: none

         export AWS_ACCESS_KEY_ID=<your-access-key-id>
         export AWS_SECRET_ACCESS_KEY=<your-secret-access-key>

   .. tab-container:: windows-ps
      :title: Windows (PS)

      .. code-block:: none

         $env:AWS_ACCESS_KEY_ID = "<your-access-key-id>"
         $env:AWS_SECRET_ACCESS_KEY = "<your-secret-access-key>"

   .. tab-container:: windows-cmd
      :title: Windows (cmd.exe)

      .. code-block:: none

         set AWS_ACCESS_KEY_ID=<your-access-key-id>
         set AWS_SECRET_ACCESS_KEY=<your-secret-access-key>

Now you are ready to start writing TileDB programs! When creating a TileDB
context or a VFS object, you need to set up a configuration object with the
following parameters for AWS S3 (supposing that your S3 buckets are on region
``us-east-1`` - you can use set an arbitrary region):

.. table:: TileDB AWS S3 config settings
    :widths: auto

    ===================================   =================
    **Parameter**                         **Value**
    -----------------------------------   -----------------
    ``"vfs.s3.scheme"``                   ``"https"``
    ``"vfs.s3.region"``                   ``"us-east-1"``
    ``"vfs.s3.endpoint_override"``        ``""``
    ``"vfs.s3.use_virtual_addressing"``   ``"true"``
    ===================================   =================

.. note::
    The above configuration parameters are currently set as shown in TileDB **by default**.
    However, we suggest to always check whether the default values are the desired ones
    for your application.

minio
-----

`minio <https://minio.io>`_ is a lightweight S3-compliant object-store.
Although it has many nice features, here we focus only on local deployment,
which is very useful if you wish to quickly test your TileDB-S3 programs
locally. See the `minio quickstart guide <https://docs.minio.io/docs/minio-quickstart-guide>`_
for installation instructions. Here is what we do to run minio on port ``9999``:

.. code-block:: bash

  $ mkdir -p /tmp/minio-data
  $ docker run -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=miniosecretkey \
        -d -p 9999:9000 minio/minio server /tmp/minio-data
  $ export AWS_ACCESS_KEY_ID=minio
  $ export AWS_SECRET_ACCESS_KEY=miniosecretkey


Once you get minio server running, you need to set the S3 configurations
as follows (below, ``<port>`` stands for the port on which you are running
the minio server, equal to ``9999`` if you run the minio docker
as shown above):

.. table:: TileDB minio S3 config settings
    :widths: auto

    ====================================   =======================
    **Parameter**                          **Value**
    ------------------------------------   -----------------------
    ``"vfs.s3.scheme"``                    ``"http"``
    ``"vfs.s3.region"``                    ``""``
    ``"vfs.s3.endpoint_override"``         ``"localhost:<port>"``
    ``"vfs.s3.use_virtual_addressing"``    ``"false"``
    ====================================   =======================

Physical Organization on S3
---------------------------

So far we explained that a TileDB array, key-value store or group
are stored as *directories* in local storage. There is no directory
concept on S3 and other similar object stores. However, S3 uses
character ``/`` in the object URIs which allows the same conceptual
organization as a directory hierarchy in local storage. At a physical
level, TileDB stores on S3 all the files it would create
locally as objects. For instance, for array ``s3://bucket/path/to/array``,
TileDB creates array schema object ``s3://bucket/path/to/array/__array_schema.tdb``,
fragment metadata object ``s3://bucket/path/to/array/<fragment>/__fragment_metadata.tdb``,
and similarly all the other files/objects. Since there is no notion of a
"directory" on S3, nothing special is persisted on S3 for directories, e.g.,
``s3://bucket/path/to/array/<fragment>/`` does not exist as an object.

The `AWS S3 CLI <https://docs.aws.amazon.com/cli/latest/reference/s3/>`_
allows you to **sync** (i.e., download) the S3 objects having a common
URI prefix to local storage, organizing them into a directory
hierarchy based on the use of ``/`` in the object URIs. This makes it
very easy to clone TileDB arrays, key-value stores or entire groups
locally from S3. For instance,
given an array ``my_array`` you created and wrote on an S3 bucket
``my_bucket``, you can clone it locally to an array ``my_local_array``
with the following command from your console:

.. code-block:: bash

   $ aws s3 sync s3://my_bucket/my_array my_local_array

After downloading an array locally, your TileDB program will function
properly by changing the array name from ``s3://my_bucket/my_array``
to ``my_local_array``, without any other modification.

S3 performance
--------------

TileDB writes the various fragment files as **append-only** objects
using the **multi-part upload** API of the
`AWS C++ SDK <https://github.com/aws/aws-sdk-cpp>`__. In addition to
enabling appends, this API renders the TileDB writes to S3 particularly
amenable to optimizations via parallelization. Since TileDB updates
arrays only by writing (appending to) new files (i.e., it never updates
a file in-place), TileDB does not need to download entire objects,
update them, and re-upload them to S3. This leads to excellent write
performance.

TileDB reads utilize the **range GET request** API of the AWS SDK, which
retrieves only the requested (contiguous) bytes from a file/object,
rather than downloading the entire file from the cloud. This results in
extremely fast subarray reads, especially because of the array
**tiling**. Recall that a tile (which groups cell values that are stored
contiguously in the file) is **the atomic unit of IO**. The range GET
API enables reading each tile from S3 in a single request.

