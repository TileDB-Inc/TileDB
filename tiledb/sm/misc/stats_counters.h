/**
 * @file   stats_all.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file contains declarations of statistics variables.
 */

#ifdef STATS_DEFINE_FUNC_STAT
// VFS
STATS_DEFINE_FUNC_STAT(vfs_abs_path)
STATS_DEFINE_FUNC_STAT(vfs_close_file)
STATS_DEFINE_FUNC_STAT(vfs_constructor)
STATS_DEFINE_FUNC_STAT(vfs_create_bucket)
STATS_DEFINE_FUNC_STAT(vfs_create_dir)
STATS_DEFINE_FUNC_STAT(vfs_create_file)
STATS_DEFINE_FUNC_STAT(vfs_destructor)
STATS_DEFINE_FUNC_STAT(vfs_empty_bucket)
STATS_DEFINE_FUNC_STAT(vfs_file_size)
STATS_DEFINE_FUNC_STAT(vfs_filelock_lock)
STATS_DEFINE_FUNC_STAT(vfs_filelock_unlock)
STATS_DEFINE_FUNC_STAT(vfs_init)
STATS_DEFINE_FUNC_STAT(vfs_is_bucket)
STATS_DEFINE_FUNC_STAT(vfs_is_dir)
STATS_DEFINE_FUNC_STAT(vfs_is_empty_bucket)
STATS_DEFINE_FUNC_STAT(vfs_is_file)
STATS_DEFINE_FUNC_STAT(vfs_ls)
STATS_DEFINE_FUNC_STAT(vfs_move_file)
STATS_DEFINE_FUNC_STAT(vfs_move_dir)
STATS_DEFINE_FUNC_STAT(vfs_open_file)
STATS_DEFINE_FUNC_STAT(vfs_read)
STATS_DEFINE_FUNC_STAT(vfs_remove_bucket)
STATS_DEFINE_FUNC_STAT(vfs_remove_file)
STATS_DEFINE_FUNC_STAT(vfs_remove_dir)
STATS_DEFINE_FUNC_STAT(vfs_supports_fs)
STATS_DEFINE_FUNC_STAT(vfs_sync)
STATS_DEFINE_FUNC_STAT(vfs_write)
STATS_DEFINE_FUNC_STAT(vfs_s3_fill_file_buffer)
STATS_DEFINE_FUNC_STAT(vfs_s3_write_multipart)
#endif

#ifdef STATS_INIT_FUNC_STAT
// VFS
STATS_INIT_FUNC_STAT(vfs_abs_path)
STATS_INIT_FUNC_STAT(vfs_close_file)
STATS_INIT_FUNC_STAT(vfs_constructor)
STATS_INIT_FUNC_STAT(vfs_create_bucket)
STATS_INIT_FUNC_STAT(vfs_create_dir)
STATS_INIT_FUNC_STAT(vfs_create_file)
STATS_INIT_FUNC_STAT(vfs_destructor)
STATS_INIT_FUNC_STAT(vfs_empty_bucket)
STATS_INIT_FUNC_STAT(vfs_file_size)
STATS_INIT_FUNC_STAT(vfs_filelock_lock)
STATS_INIT_FUNC_STAT(vfs_filelock_unlock)
STATS_INIT_FUNC_STAT(vfs_init)
STATS_INIT_FUNC_STAT(vfs_is_bucket)
STATS_INIT_FUNC_STAT(vfs_is_dir)
STATS_INIT_FUNC_STAT(vfs_is_empty_bucket)
STATS_INIT_FUNC_STAT(vfs_is_file)
STATS_INIT_FUNC_STAT(vfs_ls)
STATS_INIT_FUNC_STAT(vfs_move_file)
STATS_INIT_FUNC_STAT(vfs_move_dir)
STATS_INIT_FUNC_STAT(vfs_open_file)
STATS_INIT_FUNC_STAT(vfs_read)
STATS_INIT_FUNC_STAT(vfs_remove_bucket)
STATS_INIT_FUNC_STAT(vfs_remove_file)
STATS_INIT_FUNC_STAT(vfs_remove_dir)
STATS_INIT_FUNC_STAT(vfs_supports_fs)
STATS_INIT_FUNC_STAT(vfs_sync)
STATS_INIT_FUNC_STAT(vfs_write)
STATS_INIT_FUNC_STAT(vfs_s3_fill_file_buffer)
STATS_INIT_FUNC_STAT(vfs_s3_write_multipart)

#endif

#ifdef STATS_REPORT_FUNC_STAT
// VFS
STATS_REPORT_FUNC_STAT(vfs_abs_path)
STATS_REPORT_FUNC_STAT(vfs_close_file)
STATS_REPORT_FUNC_STAT(vfs_constructor)
STATS_REPORT_FUNC_STAT(vfs_create_bucket)
STATS_REPORT_FUNC_STAT(vfs_create_dir)
STATS_REPORT_FUNC_STAT(vfs_create_file)
STATS_REPORT_FUNC_STAT(vfs_destructor)
STATS_REPORT_FUNC_STAT(vfs_empty_bucket)
STATS_REPORT_FUNC_STAT(vfs_file_size)
STATS_REPORT_FUNC_STAT(vfs_filelock_lock)
STATS_REPORT_FUNC_STAT(vfs_filelock_unlock)
STATS_REPORT_FUNC_STAT(vfs_init)
STATS_REPORT_FUNC_STAT(vfs_is_bucket)
STATS_REPORT_FUNC_STAT(vfs_is_dir)
STATS_REPORT_FUNC_STAT(vfs_is_empty_bucket)
STATS_REPORT_FUNC_STAT(vfs_is_file)
STATS_REPORT_FUNC_STAT(vfs_ls)
STATS_REPORT_FUNC_STAT(vfs_move_file)
STATS_REPORT_FUNC_STAT(vfs_move_dir)
STATS_REPORT_FUNC_STAT(vfs_open_file)
STATS_REPORT_FUNC_STAT(vfs_read)
STATS_REPORT_FUNC_STAT(vfs_remove_bucket)
STATS_REPORT_FUNC_STAT(vfs_remove_file)
STATS_REPORT_FUNC_STAT(vfs_remove_dir)
STATS_REPORT_FUNC_STAT(vfs_supports_fs)
STATS_REPORT_FUNC_STAT(vfs_sync)
STATS_REPORT_FUNC_STAT(vfs_write)
STATS_REPORT_FUNC_STAT(vfs_s3_fill_file_buffer)
STATS_REPORT_FUNC_STAT(vfs_s3_write_multipart)
#endif

#ifdef STATS_DEFINE_COUNTER_STAT
// VFS
STATS_DEFINE_COUNTER_STAT(vfs_read_total_bytes)
STATS_DEFINE_COUNTER_STAT(vfs_write_total_bytes)
STATS_DEFINE_COUNTER_STAT(vfs_read_num_parallelized)
STATS_DEFINE_COUNTER_STAT(vfs_s3_num_parts_written)
STATS_DEFINE_COUNTER_STAT(vfs_s3_write_num_parallelized)
#endif

#ifdef STATS_INIT_COUNTER_STAT
// VFS
STATS_INIT_COUNTER_STAT(vfs_read_total_bytes)
STATS_INIT_COUNTER_STAT(vfs_write_total_bytes)
STATS_INIT_COUNTER_STAT(vfs_read_num_parallelized)
STATS_INIT_COUNTER_STAT(vfs_s3_num_parts_written)
STATS_INIT_COUNTER_STAT(vfs_s3_write_num_parallelized)
#endif

#ifdef STATS_REPORT_COUNTER_STAT
// VFS
STATS_REPORT_COUNTER_STAT(vfs_read_total_bytes)
STATS_REPORT_COUNTER_STAT(vfs_write_total_bytes)
STATS_REPORT_COUNTER_STAT(vfs_read_num_parallelized)
STATS_REPORT_COUNTER_STAT(vfs_s3_num_parts_written)
STATS_REPORT_COUNTER_STAT(vfs_s3_write_num_parallelized)
#endif