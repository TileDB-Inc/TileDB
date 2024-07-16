# Port Overlays

This directory contains the custom vcpkg port overlays we're using for building dependencies. 

## Adding a Port

> [!IMPORTANT]
> Port overlays should be used as a temporary measure to fix issues with upstream ports or to add new ports that are not yet available to vcpkg. Once the upstream ports are fixed or added, the overlay ports should be removed.

If modifying an existing port, you have to first determine the version of the port in the `microsoft/vcpkg` repository and extract it using a tool. If the port does not have a version pin in the [`vcpkg.json`](../vcpkg.json) manifest (in either a `version>=` field or an entry in the `overrides` section), browse the `microsoft/vcpkg` repository in the commit specified in the `builtin-baseline` field and copy the port directory from there.

If the port does have a version pin, and for instance we wanted to modify the curl port, which is in version 8.4.0, we would look in the [`versions/c-/curl.json` file](https://github.com/microsoft/vcpkg/blob/master/versions/c-/curl.json) and find the treeish listed for 8.4.0. One thing to pay attention to here is that there can be multiple port versions for a given dependency version. So we want to pick the highest port version for the dependency at the version we are upgrading. In our hypothetical curl case that I may have just done, this gives us a treeish value of:

   `6125c796d6e2913a89a2996d7082375ce16b02dd`

Once we have the tree-ish, we just need to be able to extract all of the files and store them in our overlay ports directory. The easiest approach for this is to use something like [this script](https://gist.github.com/mhl/498447/b245d48f2a22301415a30ca8a68241f96e0b3861) to do just that. If you put that script on your path (and remember to `chmod +x path/to/extract-tree-from-git.py`) you can follow these simple steps for updating the port:

```bash
$ rm ports/curl/*
$ cd path/to/microsoft/vcpkg
$ extract-tree-from-git.py 6125c796d6e2913a89a2996d7082375ce16b02dd path/to/tiledb/ports/curl/
$ cd path/to/tiledb
$ git add ports
$ git commit
```

After copying the port, add an entry to the table below. You should also contribute your changes to vcpkg and/or the upstream package repository.

## List of port overlays

| Port                       | Reason                                                                                                  |
|----------------------------|---------------------------------------------------------------------------------------------------------|
| `libmagic`                 | Updating to the upstream port deferred due to failures.                                                 |
| `pcre2`                    | To be removed alongside libmagic.                                                                       |
| `azure-storage-common-cpp` | Patching to disable default features on libxml2 (https://github.com/Azure/azure-sdk-for-cpp/pull/5221). |
| `libfaketime`              | Port does not yet exist upstream                                                                        |
| `vcpkg-cmake-config`       | Patching to fix build issues with CMake 3.29.1. (https://github.com/microsoft/vcpkg/pull/38017)         |
| `google-cloud-cpp`         | Patching to remove dependency on GMock. (https://github.com/microsoft/vcpkg/pull/39802)                 |
