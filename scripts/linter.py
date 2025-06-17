import os
import re
import sys
from dataclasses import dataclass, field
from typing import Collection, FrozenSet, Iterable, Mapping, Pattern

# Internal heap memory APIs must be used for memory tracking purposes
LINT_HEAP_MEMORY = 'Heap Memory'
# Custom assert routines must be used so the TILEDB_ASSERTIONS build option
# can cleanly turn them on in optimized CI runs.
LINT_ASSERT = 'Assert'

# Do not check for lint in these directories.
ignored_dirs = frozenset(["c_api", "cpp_api"])

# Do not check for lint in these files.
heap_memory_ignored_files = frozenset(
    [
        "allocate_unique.h",
        "dynamic_memory.h",
        "unit_dynamic_memory.cc",
        "heap_profiler.h",
        "heap_profiler.cc",
        "heap_memory.h",
        "heap_memory.cc",
        "stop_token.h",
    ]
)

# Match C API malloc:
regex_malloc = re.compile(r"malloc\(")

# Contains per-file exceptions to violations of "malloc".
malloc_exceptions = {
    "*": ["tdb_malloc", "tiledb_malloc"],
    "context_api.cc": ["*stats_json = static_cast<char*>(std::malloc(str.size() + 1));"]
}

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
    "*": ["tdb::make_shared", "tiledb::common::make_shared"],
}

# Match C++ unique_ptr objects.
regex_unique_ptr = re.compile(r"unique_ptr<")

# Contains per-file exceptions to violations of "make_unique".
unique_ptr_exceptions = {
    "*": ["tdb_unique_ptr", "tiledb_unique_ptr", "tdb::pmr::unique_ptr"],
    "zstd_compressor.h": ["std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> ctx_;", "std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx_;"],
    "posix.cc": ["std::unique_ptr<DIR, UniqueDIRDeleter>", "static std::unique_ptr<char, decltype(&free)> cwd_(getcwd(nullptr, 0), free);"],
    "curl.h": ["std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>"],
    "pmr.h": ["std::unique_ptr", "unique_ptr<Tp> make_unique(", "unique_ptr<Tp> emplace_unique("],
}

regex_assert = re.compile(r"^assert\(")


@dataclass
class RegexLinter:
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
class HeapMemoryLinter(RegexLinter):
    lint_kind: int = LINT_HEAP_MEMORY

    def accept_path(self, file_name: str) -> bool:
        return os.path.basename(file_name) not in heap_memory_ignored_files


@dataclass
class AssertLinter(RegexLinter):
    lint_kind: int = LINT_ASSERT

    def accept_path(self, file_name: str) -> bool:
        return True


@dataclass
class Lint:
    file_path: str
    line_num: int
    line: str
    kind: int


linters = [
    HeapMemoryLinter("malloc", regex_malloc, malloc_exceptions),
    HeapMemoryLinter("calloc", regex_calloc, calloc_exceptions),
    HeapMemoryLinter("realloc", regex_realloc, realloc_exceptions),
    HeapMemoryLinter("free", regex_free, free_exceptions),
    HeapMemoryLinter("new", regex_new),
    HeapMemoryLinter("delete", regex_delete),
    HeapMemoryLinter("make_shared<", regex_make_shared, make_shared_exceptions),
    HeapMemoryLinter("unique_ptr<", regex_unique_ptr, unique_ptr_exceptions),
    AssertLinter("assert", regex_assert)
]


def iter_file_lints(file_path: str) -> Iterable[Lint]:
    this_file_linters = [linter for linter in linters if linter.accept_path(file_path)]
    with open(file_path, encoding="utf-8") as f:
        for line_num, line in enumerate(f):
            line = line.strip()
            for linter in this_file_linters:
                if linter(file_path, line):
                    yield Lint(file_path, line_num, line, linter.lint_kind)


def iter_dir_lints(dir_path: str) -> Iterable[Lint]:
    for directory, subdirlist, file_names in os.walk(dir_path):
        if os.path.basename(directory) in ignored_dirs:
            subdirlist.clear()
        else:
            for file_name in file_names:
                if file_name.endswith(('.h', '.cc')):
                    yield from iter_file_lints(os.path.join(directory, file_name))


def explain_lint(kind):
    if kind == LINT_HEAP_MEMORY:
        print('  Source files within TileDB must use heap memory APIs\n'
              '  defined in tiledb/common/heap_memory.h.\n'
              '  This is important for tracking memory usage.\n'
              '  Correct your usage or add an exception.', file = sys.stderr)
    elif kind == LINT_ASSERT:
        print('  Source files within TileDB must use an assert routine\n'
              '  defined in `tiledb/common/assert.h`.\n'
              '  This allows all assertions to toggle with the\n'
              '  TILEDB_ASSERTIONS build configuration.\n'
              '  See `tiledb/common/assert.h` for more details about this lint.\n'
              '  Use iassert or passert instead, or add an exception.', file = sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: <root dir>")

    root_dir = os.path.abspath(sys.argv[1])
    print(f"Checking for lint in {root_dir}")

    all_lint = {}
    for lint in iter_dir_lints(root_dir):
        if lint.kind not in all_lint:
            all_lint[lint.kind] = [lint]
        else:
            all_lint[lint.kind].append(lint)

    if all_lint:
        this_filename = os.path.basename(__file__)
        for kind in all_lint.keys():
            print(f'{kind} lint detected!', file = sys.stderr)
            explain_lint(kind)

        for (kind, lint) in all_lint.items():
            print(
                f"[{this_filename}]: Detected {kind} lint:"
            )
            for l in lint:
                print(f"  {l.file_path}:{l.line_num}")
                print(f"  {l.line}")

        sys.exit(1)
    else:
        print("No lint found.")
