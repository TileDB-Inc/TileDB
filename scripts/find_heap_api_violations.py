import os
import re
import sys

# Do not check for violations in these directories.
ignored_dirs = ["c_api", "cpp_api"]

# Do not check for violations in these files.
ignored_files = [
    "heap_profiler.h",
    "heap_profiler.cc",
    "heap_memory.h",
    "heap_memory.cc",
]

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
regex_realloc = re.compile(r"[^>^\.^:]realloc\(")

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
regex_free = re.compile(r"[^_^>^\.^:]free\(")

# Contains per-file exceptions to violations of "free".
free_exceptions = {
    "*": ["tdb_free", "tiledb_free"],
    "chunked_buffer.h": ["void free()"],
}

# Match C++ new operators, examples:
#  new Foo(
#  new (std::nothro) Foo(
#  new (std::nothro) Foo[
#  new Foo (
#  new (std::nothro) Foo (
#  new (std::nothro) Foo [
regex_new = re.compile(r"new\s+(\(std::nothrow\)+)?[a-zA-Z_][a-zA-Z0-9_]*\s*(\(|\[)")

# Match C++ delete operators, examples:
#  delete Foo;
#  delete [] Foo;
#  delete Foo)
#  delete [] Foo)
#  delete *Foo;
regex_delete = re.compile(r"delete\s*(\[\])?\s+(\*)?[a-zA-Z_][a-zA-Z0-9_]*\s*(;|\))")

# Match C++ shared_ptr objects.
regex_shared_ptr = re.compile(r"shared_ptr<")

# Contains per-file exceptions to violations of "shared_ptr".
shared_ptr_exceptions = {
    "*": ["tdb_shared_ptr", "tiledb_shared_ptr"],
    "logger.h": ["std::shared_ptr<spdlog::logger> logger_"],
    "gcs.cc": ["std::shared_ptr<google::cloud::storage::oauth2::Credentials>"],
    "s3.cc": [
        "std::shared_ptr<Aws::IOStream>",
        "std::shared_ptr<Aws::Auth::AWSCredentialsProvider>",
    ],
    "s3.h": ["std::shared_ptr<S3ThreadPoolExecutor>"],
    "azure.cc": [
        "std::shared_ptr<azure::storage_lite::storage_credential>",
        "std::shared_ptr<azure::storage_lite::storage_account>",
        "azure::storage_lite::shared_access_signature_credential>"
    ],
}

# Match C++ make_shared routine.
regex_make_shared = re.compile(r"make_shared<")

# Contains per-file exceptions to violations of "make_shared".
make_shared_exceptions = {
    "*": ["tdb_make_shared", "tiledb_make_shared"],
    "s3.cc": [
        "std::make_shared<S3ThreadPoolExecutor>",
        "std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>",
        "std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>",
    ],
    "azure.cc": [
        "std::make_shared<azure::storage_lite::shared_key_credential>",
        "std::make_shared<azure::storage_lite::storage_account>",
        "std::make_shared<azure::storage_lite::tinyxml2_parser>",
        "std::make_shared<AzureRetryPolicy>",
        "std::make_shared<"
    ],
}

# Match C++ unique_ptr objects.
regex_unique_ptr = re.compile(r"unique_ptr<")

# Contains per-file exceptions to violations of "make_unique".
unique_ptr_exceptions = {
    "*": ["tdb_unique_ptr", "tiledb_unique_ptr"],
    "zstd_compressor.cc": [
        "std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> ctx(",
        "std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx(",
    ],
    "curl.h": ["std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>"],
}

# Reports a violation to stdout.
def report_violation(file_path, line, substr):
    print(
        "["
        + os.path.basename(__file__)
        + "]: "
        + "Detected '"
        + substr
        + "' heap memory API violation:"
    )
    print("  " + file_path)
    print("  " + line)


# Checks if a given line contains a violation.
def check_line(file_path, line, substr, compiled_regex, exceptions):
    if substr not in line:
        return

    line = line.strip()

    file_name = os.path.basename(file_path)

    if exceptions is not None:
        if "*" in exceptions:
            for exception in exceptions["*"]:
                if exception in line:
                    return
        if file_name in exceptions:
            for exception in exceptions[file_name]:
                if exception in line:
                    return

    if compiled_regex.search(line) is not None:
        report_violation(file_path, line, substr)
        return True

    return False


# Checks if any lines in a file contains a violation.
def check_file(file_path):
    found_violation = False
    with open(file_path) as f:
        for line in f:
            if check_line(file_path, line, "malloc", regex_malloc, malloc_exceptions):
                found_violation = True
            if check_line(file_path, line, "calloc", regex_calloc, calloc_exceptions):
                found_violation = True
            if check_line(
                file_path, line, "realloc", regex_realloc, realloc_exceptions
            ):
                found_violation = True
            if check_line(file_path, line, "free", regex_free, free_exceptions):
                found_violation = True
            if check_line(file_path, line, "new", regex_new, None):
                found_violation = True
            if check_line(file_path, line, "delete", regex_delete, None):
                found_violation = True
            if check_line(
                file_path,
                line,
                "shared_ptr<",
                regex_shared_ptr,
                shared_ptr_exceptions,
            ):
                found_violation = True
            if check_line(
                file_path,
                line,
                "make_shared<",
                regex_make_shared,
                make_shared_exceptions,
            ):
                found_violation = True
            if check_line(
                file_path,
                line,
                "unique_ptr<",
                regex_unique_ptr,
                unique_ptr_exceptions,
            ):
                found_violation = True
    return found_violation


if len(sys.argv) < 2:
    print("Usage: <root dir>")
    sys.exit(1)
root_dir = os.path.abspath(sys.argv[1])
found_violation = False
print("Checking for heap memory API violations in " + root_dir)
for directory, subdirlist, file_names in os.walk(root_dir):
    if os.path.basename(os.path.normpath(directory)) in ignored_dirs:
        continue

    for file_name in file_names:
        if file_name in ignored_files:
            continue

        if not file_name.endswith(".h") and not file_name.endswith(".cc"):
            continue

        file_path = os.path.join(directory, file_name)
        if check_file(file_path):
            found_violation = True

if found_violation:
    error_msg = (
        "Detected heap memory API violations!\n"
        "Source files within TileDB must use the heap memory APIs "
        "defined in tiledb/common/heap_memory.h\n"
        "Either correct your changes or add an exception within " + __file__
    )
    print(error_msg)
    exit(1)
else:
    print("Did not detect heap memory API violations.")
    sys.exit(0)
