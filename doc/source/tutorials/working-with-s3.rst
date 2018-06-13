Tutorial: Working with S3
=========================

This is a simple guide that demonstrates how to use TileDB on S3-compatible
backends. We will first explain how to set up an AWS account and configure
TileDB for `AWS S3 <https://aws.amazon.com/s3/>`_, and then show how to
set up and use `minio <https://minio.io>`_ locally.

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

    ===============================   ===========
    **Parameter**                     **Value**
    -------------------------------   -----------
    "vfs.s3.scheme"                   "https"
    "vfs.s3.region"                   "us-east-1"
    "vfs.s3.endpoint_override"        ""
    "vfs.s3.use_virtual_addressing"   "true"
    ===============================   ===========

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
for installation instructions.

Once you get minio server running, you need to set the S3 configurations
as follows (below, ``<port>`` stands for the port on which you are running the minio server):

.. table:: TileDB minio S3 config settings
    :widths: auto

    ===============================   ==================
    **Parameter**                     **Value**
    -------------------------------   ------------------
    "vfs.s3.scheme"                   "http"
    "vfs.s3.region"                   ""
    "vfs.s3.endpoint_override"        "localhost:<port>"
    "vfs.s3.use_virtual_addressing"   "false"
    ===============================   ==================

