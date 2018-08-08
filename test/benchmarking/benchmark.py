#!/usr/bin/env python

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


def run_benchmarks(args):
    """Runs the benchmark programs."""
    if args.benchmarks is None:
        benchmarks = list_benchmarks()
    else:
        benchmarks = args.benchmarks.split(',')

    print('Dropping caches (you may be prompted for sudo access).')
    drop_fs_caches()

    print('Running benchmarks...')
    p = ProgressBar()

    try:
        results = {}
        for b in benchmarks:
            exe = os.path.join(benchmark_build_dir, b)
            if not os.path.exists(exe):
                print('Error: no benchmark named "{}"'.format(b))
                continue

            subprocess.check_output([exe, 'setup'], cwd=benchmark_build_dir)

            times_ms = []
            for i in range(0, NUM_TRIALS):
                sync_fs()
                drop_fs_caches()
                output_json = subprocess.check_output([exe, 'run'],
                                                      cwd=benchmark_build_dir)
                result = json.loads(output_json)
                times_ms.append(result['ms'])
            results[b] = times_ms

            subprocess.check_output([exe, 'teardown'], cwd=benchmark_build_dir)
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
