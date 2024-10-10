#!/usr/bin/env python3

import shlex
import os.path
import subprocess as sp

VCPKG_DIR = "vcpkg_installed"

def vcpkg_deps():
    if not os.path.isdir(VCPKG_DIR):
        print("Unable to find `{VCPKG_DIR}` directory")
        exit(2)
    for entry in  os.listdir(VCPKG_DIR):
        libdir = os.path.join(VCPKG_DIR, entry, "lib")
        print(libdir)
        if not os.path.isdir(libdir):
            continue
        for entry in os.listdir(libdir):
            if os.path.splitext(entry)[1] != ".a":
                continue
            yield os.path.join(libdir, entry)


def main():
    libs = ["tiledb/libtiledb.a"]
    libs.extend(vcpkg_deps())
    for lib in libs:
        if not os.path.isfile(lib):
            print("Path failed: {}", lib)
            exit(2)

    cmd = "armerge --output libtiledb_bundled.a " + " ".join(libs)
    sp.check_call(cmd, shell=True)


if __name__ == "__main__":
    main()
