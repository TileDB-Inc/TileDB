TileDB patches
--------------

TileDB dependencies must be built against source packages of released software versions.

To simplify patchset maintenance, we may also fork a specific repository version in
the TileDB-Inc organization.

Several illustrative examples, using the AWS SDK dependency:
- applying a patchset to a repo fork:

    ```
    git clone https://github.com/aws/aws-sdk-cpp.git
    cd aws-sdk-cpp
    git am /path/to/TileDB/cmake/inputs/patches/ep_awssdk/awsccommon.patch
    ```

- re-creating the patchset from updated fork:

    ```
    cd aws-sdk-cpp
    # commit or cherry-pick changes
    git format-patch v0.3.0 --stdout > /path/to/TileDB/cmake/inputs/patches/ep_awssdk/awsccommon.patch
    # git add, commit, push to TileDB-Inc fork
    ```

  Note that the target branch above is for illustration only. The target version should match the version
  of the dependency used in the `cmake/Modules/Find<DEP>.cmake` file.
