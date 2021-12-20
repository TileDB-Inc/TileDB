import os
import re
import sys
from dataclasses import dataclass, field
from typing import Collection, Iterable, Mapping, Pattern

# Do not check for violations in these directories.
ignored_dirs = frozenset(["c_api", "cpp_api"])

# Do not check for violations in these files.
ignored_files = frozenset(
    [
        "allocate_unique.h",
        "dynamic_memory.h",
        "unit_dynamic_memory.cc",
        "heap_profiler.h",
        "heap_profiler.cc",
        "heap_memory.h",
        "heap_memory.cc",
    ]
)

# Match C API malloc:
regex_malloc = re.compile(r"malloc\(")

# Contains per-file exceptions to violations of "malloc".
malloc_exceptions = {"*": ["tdb_malloc", "tiledb_malloc"]}

# Match C API calloc:
regex_calloc = re.compile(r"calloc\(")

# Contains per-file exceptions to violations of "calloc".
calloc_exceptions = {"*": ["tdb_calloc", "tiledb_calloc"]}

# Match C API realloc, examples:
# = realloc(
# , realloc(
# ,realloc(
#
# Excludes:
# .realloc(   # e.g. member access, `foo.realloc(`
# >realloc(   # e.g. pointer member access, `foo->realloc(`
# :realloc(   # e.g. member routine defintion, `Class::realloc()`
regex_realloc = re.compile(r"[^>.:]realloc\(")

# Contains per-file exceptions to violations of "realloc".
realloc_exceptions = {
    "*": ["tdb_realloc", "tiledb_realloc"],
    "buffer.h": ["Status realloc(uint64_t nbytes)"],
}

# Match C API free, examples:
# free(
# , free(
# ,free(
#
# Excludes:
# _free(   # e.g. `EVP_CIPHER_CTX_free`
# .free(   # e.g. member access, `foo.free(`
# >free(   # e.g. pointer member access, `foo->free(`
# :free(   # e.g. member routine defintion, `Class::free()`
regex_free = re.compile(r"[^_>.:]free\(")

# Contains per-file exceptions to violations of "free".
free_exceptions = {
    "*": ["tdb_free", "tiledb_free"],
}

# Match C++ new operators, examples:
#  new Foo(
#  new (std::nothrow) Foo(
#  new (std::nothrow) Foo[
#  new Foo (
#  new (std::nothrow) Foo (
#  new (std::nothrow) Foo [
regex_new = re.compile(r"new\s+(\(std::nothrow\))?\w+\s*[([]")

# Match C++ delete operators, examples:
#  delete Foo;
#  delete [] Foo;
#  delete Foo)
#  delete [] Foo)
#  delete *Foo;
regex_delete = re.compile(r"delete\s*(\[\])?\s+\*?\w+\s*[;)]")

# Match C++ make_shared routine.
regex_make_shared = re.compile(r"std::make_shared<")

# Contains per-file exceptions to violations of "make_shared".
make_shared_exceptions = {
    "*": ["tdb::make_shared"],
    "*": ["tiledb::common::make_shared"],
}

# Match C++ unique_ptr objects.
regex_unique_ptr = re.compile(r"unique_ptr<")

# Contains per-file exceptions to violations of "make_unique".
unique_ptr_exceptions = {
    "*": ["tdb_unique_ptr", "tiledb_unique_ptr"],
    "zstd_compressor.h": ["std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> ctx_;", "std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx_;"],
    "curl.h": ["std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>"],
}


@dataclass
class ViolationChecker:
    substr: str
    regex: Pattern[str]
    exceptions: Mapping[str, Collection[str]] = field(default_factory=dict)

    def __call__(self, file_path: str, line: str) -> bool:
        if self.substr not in line:
            return False

        for key in "*", os.path.basename(file_path):
            for exception in self.exceptions.get(key, ()):
                if exception in line:
                    return False

        return self.regex.search(line) is not None


@dataclass
class Violation:
    file_path: str
    line_num: int
    line: str
    checker: ViolationChecker


violation_checkers = [
    ViolationChecker("malloc", regex_malloc, malloc_exceptions),
    ViolationChecker("calloc", regex_calloc, calloc_exceptions),
    ViolationChecker("realloc", regex_realloc, realloc_exceptions),
    ViolationChecker("free", regex_free, free_exceptions),
    ViolationChecker("new", regex_new),
    ViolationChecker("delete", regex_delete),
    ViolationChecker("make_shared<", regex_make_shared, make_shared_exceptions),
    ViolationChecker("unique_ptr<", regex_unique_ptr, unique_ptr_exceptions),
]


def iter_file_violations(file_path: str) -> Iterable[Violation]:
    with open(file_path) as f:
        for line_num, line in enumerate(f):
            line = line.strip()
            for checker in violation_checkers:
                if checker(file_path, line):
                    yield Violation(file_path, line_num, line, checker)


def iter_dir_violations(dir_path: str) -> Iterable[Violation]:
    for directory, subdirlist, file_names in os.walk(dir_path):
        if os.path.basename(directory) not in ignored_dirs:
            for file_name in file_names:
                if file_name not in ignored_files and file_name.endswith((".h", ".cc")):
                    yield from iter_file_violations(os.path.join(directory, file_name))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: <root dir>")

    root_dir = os.path.abspath(sys.argv[1])
    print(f"Checking for heap memory API violations in {root_dir}")
    violations = list(iter_dir_violations(root_dir))
    if violations:
        this_filename = os.path.basename(__file__)
        for violation in violations:
            print(
                f"[{this_filename}]: Detected {violation.checker.substr!r} heap memory API violation:"
            )
            print(f"  {violation.file_path}:{violation.line_num}")
            print(f"  {violation.line}")
        sys.exit(
            "Detected heap memory API violations!\n"
            "Source files within TileDB must use the heap memory APIs "
            "defined in tiledb/common/heap_memory.h\n"
            "Either correct your changes or add an exception within " + __file__
        )
    else:
        print("Did not detect heap memory API violations.")
