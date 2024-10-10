#!/usr/bin/env python3

import shlex
import os.path
import subprocess as sp

DEFAULT_LINK_FILE = "examples/cpp_api/CMakeFiles/groups_cpp.dir/link.txt"
DEFAULT_RELATIVE_TO = "./examples/cpp_api/"

def main():
    with open(DEFAULT_LINK_FILE) as handle:
        cmd = handle.read()
    args = shlex.split(cmd)
    libs = []
    for arg in args:
        if arg.endswith(".a") and arg not in libs:
            libs.append(arg)
    rel_to = os.path.abspath(DEFAULT_RELATIVE_TO)
    abs_libs = []
    for lib in libs:
        lib = os.path.abspath(os.path.join(rel_to, lib))
        if not os.path.isfile(lib):
            print("Missing library: {}".format(lib))
        if lib not in abs_libs:
            abs_libs.append(lib)

    cmd = " ".join([
        "armerge",
        "--output libtiledb_bundled.a"
    ] + abs_libs)
    sp.check_call(cmd, shell=True)

if __name__ == "__main__":
    main()
