#!/usr/bin/env python3

import argparse
import glob
import json
import os
import subprocess
import sys
import threading
import time

benchmark_src_dir = os.path.abspath('src')
benchmark_build_dir = os.path.abspath('build')

NUM_TRIALS = 3

if os.name == 'posix':
    if sys.platform == 'darwin':
        os_name = 'mac'
        libtiledb_name = 'libtiledb.dylib'
    else:
        os_name = 'linux'
        libtiledb_name = 'libtiledb.so'
elif os.name == 'nt':
    os_name = 'windows'
    libtiledb_name = 'tiledb.dll'


class ProgressBar(threading.Thread):
    def __init__(self):
        super(ProgressBar, self).__init__()
        self.running = False
        self.start()

    def start(self):
        self.running = True
        super(ProgressBar, self).start()

    def run(self):
        chars = ['-', '\\', '|', '/']
        i = 0
        while self.running:
            sys.stdout.write('\r{}'.format(chars[i]))
            sys.stdout.flush()
            i = (i + 1) % 4
            time.sleep(0.2)

    def stop(self):
        self.running = False
        self.join()
        print('\rDone.')
        sys.stdout.flush()


def sync_fs():
    """Syncs local filesystem pending writes."""
    if os_name == 'windows':
        print('WARNING: FS sync unimplemented')
    else:
        subprocess.call(['sudo', 'sync'])


def drop_fs_caches():
    """Drops local filesystem caches."""
    if os_name == 'linux':
        subprocess.call(
            ['sudo', 'sh', '-c', 'echo 3 >/proc/sys/vm/drop_caches'])
    elif os_name == 'mac':
        subprocess.call(['sudo', 'purge'])
    else:
        print('WARNING: FS cache drop unimplemented')


def find_tiledb_path(args):
    """
    Returns the path to the TileDB installation, or None if it can't be determined.

    :param args: argparse args instance
    :return: path or None
    """
    dirs = [args.tiledb]
    if 'TILEDB_PATH' in os.environ:
        dirs.append(os.environ['TILEDB_PATH'])
    for d in dirs:
        libdir = os.path.join(d, 'lib')
        if os.path.exists(os.path.join(libdir, libtiledb_name)):
            return os.path.abspath(d)
    return None


def list_benchmarks(show=False):
    """Returns a list of benchmark program names."""
    executables = glob.glob(os.path.join(benchmark_build_dir, 'bench_*'))
    names = []
    for exe in sorted(executables):
        is_exec = os.path.isfile(exe) and os.access(exe, os.X_OK)
        if is_exec:
            name = os.path.basename(exe)
            names.append(name)

    if show:
        print('{} benchmarks:'.format(len(names)))
        for name in names:
            print('  {}'.format(name))

    return names


def build_benchmarks(args):
    """Builds all the benchmark programs."""
    if not os.path.exists(benchmark_build_dir):
        os.mkdir(benchmark_build_dir)
    tiledb_path = find_tiledb_path(args)
    print('Building benchmarks...')
    p = ProgressBar()
    try:
        subprocess.check_output(
            ['cmake', '-DCMAKE_PREFIX_PATH={}'.format(tiledb_path),
             benchmark_src_dir],
            cwd=benchmark_build_dir)
        subprocess.check_output(['make', '-j4'], cwd=benchmark_build_dir)
    finally:
        p.stop()


def print_results(results):
    "Prints benchmark timing results."
    print('Reporting minimum time of {} runs for each benchmark:'.format(
        NUM_TRIALS))
    print('-' * 93)
    for bench in sorted(results.keys()):
        print('{:<30s}{:>60d} ms'.format(bench, min(results[bench])))


def get_bench_env(args):
    """Returns environment dict with BENCH_* variables set from args."""
    env = os.environ.copy()
    if args.uri_prefix:
        env['BENCH_URI_PREFIX'] = args.uri_prefix
    if args.s3_endpoint:
        env['BENCH_S3_ENDPOINT'] = args.s3_endpoint
    if args.s3_region:
        env['BENCH_S3_REGION'] = args.s3_region
    if args.s3_scheme:
        env['BENCH_S3_SCHEME'] = args.s3_scheme
    if args.s3_virtual_addressing:
        env['BENCH_S3_USE_VIRTUAL_ADDRESSING'] = args.s3_virtual_addressing
    if args.rest_server:
        env['BENCH_REST_SERVER'] = args.rest_server
    if args.rest_token:
        env['BENCH_REST_TOKEN'] = args.rest_token
    return env


def is_remote_backend(args):
    """Returns True if the benchmark targets a remote backend (S3 or REST)."""
    return args.backend in ('s3', 'rest')


def run_benchmarks(args):
    """Runs the benchmark programs."""
    if args.benchmarks is None:
        benchmarks = list_benchmarks()
    else:
        benchmarks = args.benchmarks.split(',')

    remote = is_remote_backend(args)
    env = get_bench_env(args)

    if not remote:
        print('Dropping caches (you may be prompted for sudo access).')
        drop_fs_caches()
    else:
        print('Using {} backend -- skipping local cache operations.'.format(
            args.backend))

    print('Running benchmarks...')
    p = ProgressBar()

    try:
        results = {}
        for b in benchmarks:
            exe = os.path.join(benchmark_build_dir, b)
            if not os.path.exists(exe):
                print('Error: no benchmark named "{}"'.format(b))
                continue

            subprocess.check_output([exe, 'setup'], cwd=benchmark_build_dir,
                                    env=env)

            times_ms = []
            for i in range(0, NUM_TRIALS):
                if not remote:
                    sync_fs()
                    drop_fs_caches()
                output_json = subprocess.check_output([exe, 'run'],
                                                      cwd=benchmark_build_dir,
                                                      env=env)
                result = json.loads(output_json)
                times_ms.append(result['runtime_ms'])
            results[b] = times_ms

            subprocess.check_output([exe, 'teardown'], cwd=benchmark_build_dir,
                                    env=env)
    finally:
        p.stop()

    print_results(results)


def main():
    parser = argparse.ArgumentParser(description='Runs TileDB benchmarks.')
    parser.add_argument('--tiledb', metavar='PATH',
                        default='../../dist',
                        help='Path to TileDB installation directory. You can '
                             'also set the TILEDB_PATH environment variable.')
    parser.add_argument('-l', '--list', action='store_true', default=False,
                        help='List all available benchmarks and exit.')
    parser.add_argument('-b', '--benchmarks', metavar='NAMES',
                        help='If given, one or more comma-separated names of '
                             'benchmarks to run.')
    parser.add_argument('--backend', choices=['local', 's3', 'rest'],
                        default='local',
                        help='Storage backend: local (default), s3, or rest.')
    parser.add_argument('--uri-prefix', metavar='URI',
                        help='URI prefix for array names '
                             '(e.g. "s3://bucket/bench/" or '
                             '"tiledb://namespace/").')
    parser.add_argument('--s3-endpoint', metavar='HOST',
                        help='S3 endpoint override (e.g. localhost:9999).')
    parser.add_argument('--s3-region', metavar='REGION',
                        help='S3 region (e.g. us-east-1).')
    parser.add_argument('--s3-scheme', metavar='SCHEME',
                        help='S3 connection scheme (e.g. https).')
    parser.add_argument('--s3-virtual-addressing', metavar='BOOL',
                        help='S3 virtual host-style addressing (true/false).')
    parser.add_argument('--rest-server', metavar='URL',
                        help='REST server address '
                             '(e.g. https://api.tiledb.com).')
    parser.add_argument('--rest-token', metavar='TOKEN',
                        help='REST API token.')
    args = parser.parse_args()

    if find_tiledb_path(args) is None:
        print('Error: TileDB installation not found in directory \'{}\''.format(
            args.tiledb))
        sys.exit(1)

    build_benchmarks(args)

    if args.list:
        list_benchmarks(show=True)
        sys.exit(0)

    run_benchmarks(args)


if __name__ == '__main__':
    main()
