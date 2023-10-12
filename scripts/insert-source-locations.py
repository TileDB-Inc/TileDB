#!/usr/bin/env python3

import os
import re
import sys

CPP_EXTENSIONS = set([".cpp", ".cc"])

ANCHOR_NAMES = [
  "api_entry",
  "api_entry_plain",
  "api_entry_void",
  "api_entry_with_context",
  "api_entry_context",
  "api_entry_error"
]

ANCHOR = "(" + "|".join(ANCHOR_NAMES) + ")"
WRAPPER_RE = re.compile(ANCHOR + "\<[\sa-zA-Z0-9:_]+\>\(", re.MULTILINE)


def process_file(fname):
  with open(fname) as handle:
    source = handle.read()

  to_write = []
  curr_pos = 0
  while True:
    match = WRAPPER_RE.search(source, pos=curr_pos)
    if match is None:
      break
    to_write.append(source[curr_pos:match.span(0)[1]])
    to_write.append("TILEDB_SOURCE_LOCATION()")
    curr_pos = match.span(0)[1]
    if source[curr_pos] != ")":
      to_write.append(", ")

  if not len(to_write):
    # No instances found in this file, avoid the unnecessary write.
    return

  to_write.append(source[curr_pos:])

  print("Rewriting: " + fname)
  new_source = "".join(to_write)
  with open(fname, "w") as handle:
    handle.write(new_source)


def main():
  if len(sys.argv) != 2:
    print("usage: {} BASE_DIRECTORY".format(sys.argv[0]))
    exit(1)

  for path, dnames, fnames in os.walk(sys.argv[1]):
    for fname in fnames:
      # Don't patch the definitions
      if fname == "exception_wrapper.h":
        continue
      if os.path.splitext(fname)[-1] not in CPP_EXTENSIONS:
        continue
      fname = os.path.join(path, fname)
      process_file(fname)


if __name__ == "__main__":
  main()
