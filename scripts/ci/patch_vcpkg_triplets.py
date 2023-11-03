import os

workspace = os.curdir
triplet_paths = [os.path.join(workspace, "ports", "triplets")]

for triplet_path in triplet_paths:
  for path, dnames, fnames in os.walk(triplet_path):
    for fname in fnames:
      fname = os.path.join(path, fname)
      print(fname)
      with open(fname, "rb") as handle:
        contents = handle.read()
      # If a file is already patched we skip patching it again because
      # this affects the hashes calculated by vcpkg for caching
      if b"VCPKG_BUILD_TYPE release" in contents:
        continue
      contents += b"\nset(VCPKG_BUILD_TYPE release)\n"
      with open(fname, "wb") as handle:
        handle.write(contents)
