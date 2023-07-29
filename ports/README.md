Port Overlays
===

This directory contains the custom port overlays we're using for building depdencies. We're using overlays because some of our dependencies are quite old and getting working combinations from modern vcpkg was proving untenable. Overtime as our dependency versions are modernized, most if not all of the overlay ports will likely be removed.

Updating a Port
---

The easiest way to update a port is to find the version of the port in the `microsoft/vcpkg` repository and extract it using a tool. So, for instance if we wanted to update our curl dependency from 7.80.0 to 7.84.0, we would look in the `path/to/microsoft/vcpkg/versions/c-/curl.json` file and find the tree-ish listed for 7.84.0. One thing to pay attention to here is that there can be multiple port versions for a given dependency version. So we want to pick the higest port version for the dependency at the version we are upgrading. In our hypothetical curl case that I may have just done, this gives us a treeish value of:

   `588fa4742c417db9d7c0f89e652b618296388d1e`

Once we have the tree-ish, we just need to be able to extract all of the files and store them in our overlay ports directory. The easiest approach for this is to use something like [this script](https://gist.github.com/mhl/498447/b245d48f2a22301415a30ca8a68241f96e0b3861) to do just that. If you put that script on your path (and remember to `chmod +x path/to/extract-tree-from-git.py`) you can follow these simple steps for updating the port:

```bash
$ rm ports/curl/*
$ cd path/to/microsoft/vcpkg
$ extract-tree-from-git.py 588fa4742c417db9d7c0f89e652b618296388d1e path/to/tiledb/ports/curl/
$ cd path/to/tiledb
$ git add ports
$ git commit
```
