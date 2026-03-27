#!/usr/bin/env python3
#
# benchmark.py — Consolidated TileDB coroutine benchmark suite.
#
# Runs benchmarks in sync and/or coroutine mode and reports a comparison.
# Only includes benchmarks that use refactored readers (DenseReader,
# SparseUnorderedWithDupsReader, SparseGlobalOrderReader) which support
# the coroutine pipeline. Legacy Reader benchmarks (sparse + ROW_MAJOR)
# are excluded since coroutines are disabled for that code path.
#
# Usage:
#   python3 benchmark.py --tiledb /path/to/tiledb/install
#   python3 benchmark.py --tiledb /path/to/install --mode compare
#   python3 benchmark.py --tiledb /path/to/install --mode sync
#   python3 benchmark.py --tiledb /path/to/install --backend s3 \
#       --uri-prefix s3://bucket/bench/ --s3-region us-east-1
#   python3 benchmark.py --tiledb /path/to/install --trials 5
#   python3 benchmark.py --tiledb /path/to/install --csv results.csv

import argparse
import csv
import glob
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime

benchmark_src_dir = os.path.abspath('src')
benchmark_build_dir = os.path.abspath('build')

# Coroutine benchmark suite — all read benchmarks that support the coroutine
# pipeline. Sparse benchmarks default to TILEDB_UNORDERED layout (routing to
# refactored readers). Set BENCH_SPARSE_READ_LAYOUT=row_major to test the
# legacy Reader path instead.
COROUTINE_SUITE = [
    # --- Targeted coroutine benchmarks ---
    'bench_coroutine_worst_case',     # Dense, 1 attr, LZ4 (minimal overlap)
    'bench_coroutine_best_case',      # Dense, 10 attrs, ZSTD (max overlap)
    'bench_coroutine_average_case',   # Sparse UNORDERED, 5 attrs, mixed

    # --- Dense read benchmarks (DenseReader) ---
    'bench_dense_3d_read',
    'bench_dense_multi_attr',
    'bench_dense_multi_fragment_read',
    'bench_dense_read_col_major',
    'bench_dense_read_incomplete',
    'bench_dense_read_large_tile',
    'bench_dense_read_nullable',
    'bench_dense_read_qc_combined',
    'bench_dense_read_small_tile',
    'bench_dense_read_varlen',

    # --- Sparse read benchmarks (default UNORDERED → refactored readers) ---
    'bench_sparse_multi_attr',
    'bench_sparse_multi_fragment_read',
    'bench_sparse_read_incomplete',
    'bench_sparse_read_large_tile',
    'bench_sparse_read_nullable',
    'bench_sparse_read_qc_combined',
    'bench_sparse_read_query_condition',
    'bench_sparse_read_small_tile',
    'bench_sparse_read_unordered',
    'bench_sparse_read_varlen',
    'bench_sparse_string_dim_read',
]

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
        print('\r', end='')
        sys.stdout.flush()


def sync_fs():
    if os_name == 'windows':
        print('WARNING: FS sync unimplemented')
    else:
        subprocess.call(['sudo', 'sync'])


def drop_fs_caches():
    if os_name == 'linux':
        subprocess.call(
            ['sudo', 'sh', '-c', 'echo 3 >/proc/sys/vm/drop_caches'])
    elif os_name == 'mac':
        subprocess.call(['sudo', 'purge'])
    else:
        print('WARNING: FS cache drop unimplemented')


def find_tiledb_path(args):
    dirs = [args.tiledb]
    if 'TILEDB_PATH' in os.environ:
        dirs.append(os.environ['TILEDB_PATH'])
    for d in dirs:
        libdir = os.path.join(d, 'lib')
        if os.path.exists(os.path.join(libdir, libtiledb_name)):
            return os.path.abspath(d)
    return None


def list_benchmarks(show=False):
    executables = glob.glob(os.path.join(benchmark_build_dir, 'bench_*'))
    names = []
    for exe in sorted(executables):
        is_exec = os.path.isfile(exe) and os.access(exe, os.X_OK)
        if is_exec:
            name = os.path.basename(exe)
            names.append(name)

    if show:
        print('{} benchmarks built:'.format(len(names)))
        for name in names:
            suite_tag = '  [coroutine suite]' if name in COROUTINE_SUITE else ''
            print('  {}{}'.format(name, suite_tag))

    return names


def build_benchmarks(args):
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


def get_bench_env(args, coroutine=False):
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

    env['TILEDB_SM_QUERY_READER_USE_COROUTINE_PIPELINE'] = \
        'true' if coroutine else 'false'
    return env


def is_remote_backend(args):
    return args.backend in ('s3', 'rest')


def run_single_mode(benchmarks, args, coroutine=False):
    """Runs all benchmarks in one mode, returns {name: [times_ms]}."""
    remote = is_remote_backend(args)
    env = get_bench_env(args, coroutine=coroutine)
    mode_name = 'coroutine' if coroutine else 'sync'

    if not remote:
        drop_fs_caches()

    results = {}
    for b in benchmarks:
        exe = os.path.join(benchmark_build_dir, b)
        if not os.path.exists(exe):
            print('\n  WARNING: {} not found, skipping'.format(b))
            continue

        sys.stdout.write('\r  [{:>9s}] {}...'.format(mode_name, b))
        sys.stdout.flush()

        try:
            subprocess.check_output(
                [exe, 'setup'], cwd=benchmark_build_dir,
                env=env, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print('\n  {} [{}]: setup FAILED'.format(b, mode_name))
            results[b] = ['FAIL']
            continue

        times_ms = []
        failed = False
        for i in range(args.trials):
            if not remote:
                sync_fs()
                drop_fs_caches()
            try:
                output_json = subprocess.check_output(
                    [exe, 'run'], cwd=benchmark_build_dir,
                    env=env, stderr=subprocess.STDOUT)
                result = json.loads(output_json)
                times_ms.append(result['runtime_ms'])
            except (subprocess.CalledProcessError, json.JSONDecodeError,
                    KeyError):
                print('\n  {} [{}]: run FAILED (trial {})'.format(
                    b, mode_name, i + 1))
                times_ms = ['FAIL']
                failed = True
                break

        try:
            subprocess.check_output(
                [exe, 'teardown'], cwd=benchmark_build_dir,
                env=env, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            pass

        if not failed:
            results[b] = times_ms
        else:
            results[b] = ['FAIL']

    print('\r' + ' ' * 80 + '\r', end='')
    return results


def print_single_results(results, mode_name, num_trials):
    print()
    print('=' * 72)
    print('  {} Results ({} trials, minimum reported)'.format(
        mode_name.title(), num_trials))
    print('=' * 72)
    print()
    print('{:<40s} {:>10s}  {:>20s}'.format(
        'Benchmark', 'Min (ms)', 'All trials'))
    print('{:<40s} {:>10s}  {:>20s}'.format('-' * 40, '-' * 10, '-' * 20))

    for b in COROUTINE_SUITE:
        if b not in results:
            continue
        times = results[b]
        if times == ['FAIL']:
            print('{:<40s} {:>10s}'.format(b, 'FAIL'))
        else:
            trial_str = ', '.join(str(t) for t in times)
            print('{:<40s} {:>10d}  {:>20s}'.format(
                b, min(times), trial_str))
    print()


def print_comparison(sync_results, coro_results, num_trials):
    print()
    print('=' * 82)
    print('  Coroutine Pipeline Benchmark Comparison')
    print('  Date:   {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('  Trials: {} (reporting minimum)'.format(num_trials))
    print('=' * 82)
    print()
    print('{:<40s} {:>10s} {:>10s} {:>10s}'.format(
        'Benchmark', 'Sync (ms)', 'Coro (ms)', 'Change'))
    print('{:<40s} {:>10s} {:>10s} {:>10s}'.format(
        '-' * 40, '-' * 10, '-' * 10, '-' * 10))

    total_sync = 0
    total_coro = 0
    count_valid = 0

    for b in COROUTINE_SUITE:
        sync_t = sync_results.get(b, ['SKIP'])
        coro_t = coro_results.get(b, ['SKIP'])

        sync_ok = sync_t != ['FAIL'] and sync_t != ['SKIP']
        coro_ok = coro_t != ['FAIL'] and coro_t != ['SKIP']

        if sync_ok:
            sync_min = min(sync_t)
            sync_str = str(sync_min)
        else:
            sync_min = None
            sync_str = 'FAIL' if sync_t == ['FAIL'] else 'SKIP'

        if coro_ok:
            coro_min = min(coro_t)
            coro_str = str(coro_min)
        else:
            coro_min = None
            coro_str = 'FAIL' if coro_t == ['FAIL'] else 'SKIP'

        if sync_min and coro_min and sync_min > 0:
            pct = ((sync_min - coro_min) / sync_min) * 100
            if abs(pct) < 2.0:
                change_str = '~0%'
            elif pct > 0:
                change_str = '-{:.1f}%'.format(pct)
            else:
                change_str = '+{:.1f}%'.format(-pct)
            total_sync += sync_min
            total_coro += coro_min
            count_valid += 1
        else:
            change_str = 'N/A'

        print('{:<40s} {:>10s} {:>10s} {:>10s}'.format(
            b, sync_str, coro_str, change_str))

    print()
    if count_valid > 0 and total_sync > 0:
        overall_pct = ((total_sync - total_coro) / total_sync) * 100
        print('{:<40s} {:>10d} {:>10d} {:>9.1f}%'.format(
            'TOTAL ({} benchmarks)'.format(count_valid),
            total_sync, total_coro, -overall_pct))
    print()
    print('Negative % = coroutine is faster. Positive % = regression.')
    print('=' * 82)

    return total_sync, total_coro, count_valid


def write_csv(filename, sync_results, coro_results):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'benchmark', 'sync_min_ms', 'coro_min_ms',
            'change_pct', 'sync_trials', 'coro_trials'])

        for b in COROUTINE_SUITE:
            sync_t = sync_results.get(b, ['SKIP'])
            coro_t = coro_results.get(b, ['SKIP'])

            sync_ok = sync_t not in (['FAIL'], ['SKIP'])
            coro_ok = coro_t not in (['FAIL'], ['SKIP'])

            sync_min = min(sync_t) if sync_ok else None
            coro_min = min(coro_t) if coro_ok else None

            if sync_min and coro_min and sync_min > 0:
                pct = ((sync_min - coro_min) / sync_min) * 100
                pct_str = '{:.1f}'.format(-pct)
            else:
                pct_str = 'N/A'

            sync_str = str(sync_min) if sync_min else (
                'FAIL' if sync_t == ['FAIL'] else 'SKIP')
            coro_str = str(coro_min) if coro_min else (
                'FAIL' if coro_t == ['FAIL'] else 'SKIP')

            sync_trials = ' '.join(str(t) for t in sync_t) if sync_ok else ''
            coro_trials = ' '.join(str(t) for t in coro_t) if coro_ok else ''

            writer.writerow([
                b, sync_str, coro_str, pct_str, sync_trials, coro_trials])

    print('CSV written to: {}'.format(filename))


def run_benchmarks(args):
    if args.benchmarks is not None:
        benchmarks = args.benchmarks.split(',')
    else:
        benchmarks = list(COROUTINE_SUITE)

    remote = is_remote_backend(args)
    mode = args.mode

    print()
    print('Suite: {} benchmarks'.format(len(benchmarks)))
    print('Mode:  {}'.format(mode))
    print('Backend: {}'.format(args.backend))
    if remote:
        print('URI prefix: {}'.format(args.uri_prefix or '(not set)'))
    print('Trials: {}'.format(args.trials))
    print()

    if mode == 'sync':
        print('Running sync baseline...')
        sync_results = run_single_mode(benchmarks, args, coroutine=False)
        print_single_results(sync_results, 'sync', args.trials)
    elif mode == 'coroutine':
        print('Running coroutine pipeline...')
        coro_results = run_single_mode(benchmarks, args, coroutine=True)
        print_single_results(coro_results, 'coroutine', args.trials)
    elif mode == 'compare':
        print('Running sync baseline...')
        sync_results = run_single_mode(benchmarks, args, coroutine=False)

        print('Running coroutine pipeline...')
        coro_results = run_single_mode(benchmarks, args, coroutine=True)

        print_comparison(sync_results, coro_results, args.trials)

        if args.csv:
            write_csv(args.csv, sync_results, coro_results)


def main():
    parser = argparse.ArgumentParser(
        description='TileDB Coroutine Benchmark Suite. Runs read benchmarks '
                    'in sync and coroutine modes to measure pipeline '
                    'improvements.')
    parser.add_argument('--tiledb', metavar='PATH',
                        default='../../dist',
                        help='Path to TileDB installation directory. You can '
                             'also set the TILEDB_PATH environment variable.')
    parser.add_argument('-l', '--list', action='store_true', default=False,
                        help='List all available benchmarks and exit.')
    parser.add_argument('-b', '--benchmarks', metavar='NAMES',
                        help='Comma-separated benchmark names to run. '
                             'Overrides the default suite.')
    parser.add_argument('--mode', choices=['sync', 'coroutine', 'compare'],
                        default='compare',
                        help='Run mode: sync (baseline only), coroutine '
                             '(pipeline only), or compare (both). '
                             'Default: compare.')
    parser.add_argument('--trials', type=int, default=3,
                        help='Number of trials per benchmark (default: 3).')
    parser.add_argument('--csv', metavar='FILE',
                        help='Write results to CSV file (compare mode only).')
    parser.add_argument('--backend', choices=['local', 's3', 'rest'],
                        default='local',
                        help='Storage backend: local (default), s3, or rest.')
    parser.add_argument('--uri-prefix', metavar='URI',
                        help='URI prefix for array names '
                             '(e.g. "s3://bucket/bench/").')
    parser.add_argument('--s3-endpoint', metavar='HOST',
                        help='S3 endpoint override.')
    parser.add_argument('--s3-region', metavar='REGION',
                        help='S3 region (e.g. us-east-1).')
    parser.add_argument('--s3-scheme', metavar='SCHEME',
                        help='S3 connection scheme (e.g. https).')
    parser.add_argument('--s3-virtual-addressing', metavar='BOOL',
                        help='S3 virtual host-style addressing (true/false).')
    parser.add_argument('--rest-server', metavar='URL',
                        help='REST server address.')
    parser.add_argument('--rest-token', metavar='TOKEN',
                        help='REST API token.')
    args = parser.parse_args()

    if find_tiledb_path(args) is None:
        print('Error: TileDB installation not found in directory \'{}\''
              .format(args.tiledb))
        sys.exit(1)

    build_benchmarks(args)

    if args.list:
        list_benchmarks(show=True)
        sys.exit(0)

    run_benchmarks(args)


if __name__ == '__main__':
    main()
