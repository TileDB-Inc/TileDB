/**
 * @file unit_mgc_dict.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Simple unit test comparing external magic.mgc data to expanded
 * internal data created from that magic.mgc and verifying a few
 * magic checks return same values using both embedded and external data.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/misc/mgc_dict.h"
#include "tiledb/sm/misc/types.h"

#include <inttypes.h>
#include <memory.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

#ifndef TILEDB_PATH_TO_MAGIC_MGC
#error "TILEDB_PATH_TO_MAGIC_MGC not defined!"
#endif

using tiledb::sm::magic_dict;

int check_embedded_data_validity() {
  FILE* infile = nullptr;
  infile = fopen(TILEDB_PATH_TO_MAGIC_MGC, "rb");
  if (!infile) {
    fprintf(stderr, "ERROR: Unable to open %s\n", TILEDB_PATH_TO_MAGIC_MGC);
    return 1;
  }

  fseek(infile, 0L, SEEK_END);
  uint64_t magic_mgc_len = ftell(infile);
  fseek(infile, 0L, SEEK_SET);

  char* magic_mgc_data = tdb_new_array(char, magic_mgc_len);
  if (fread(magic_mgc_data, 1, magic_mgc_len, infile) != magic_mgc_len) {
    fprintf(stderr, "ERROR reading data from %s\n", TILEDB_PATH_TO_MAGIC_MGC);
    return 4;
  }
  fclose(infile);

  auto magic_mgc_embedded_data = tiledb::sm::magic_dict::expanded_buffer();
  if (magic_mgc_embedded_data->size() != magic_mgc_len) {
    fprintf(
        stderr,
        "ERROR magic.mgc data len (%" PRIu64
        ") does not match embedded data length (%" PRIu64 ")\n",
        magic_mgc_len,
        static_cast<uint64_t>(magic_mgc_embedded_data->size()));
    return 7;
  }

  if (memcmp(magic_mgc_data, magic_mgc_embedded_data->data(), magic_mgc_len)) {
    fprintf(stderr, "ERROR magic.mgc data different from embedded data\n");
    return 10;
  }

  return 0;
}

char empty_txt[] = {""};  // further below, treated differently from others here
// data extracted from similarly named files to be found in test\input\files\*
char fileapi0_csv[] = {
    // note that clang format broke lines for -this one' but not others.
    '\x72',
    '\x6f',
    '\x77',
    '\x73',
    '\x2c',
    '\x63',
    '\x6f',
    '\x6c',
    '\x73',
    '\x2c',
    '\x61',
    '\x0a',
    '\x31',
    '\x2c',
    '\x31',
    '\x2c',
    '\x31',
    '\x0a',
};
char fileapi1_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c',
    '\x73', '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c',
    '\x31', '\x0a', '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a',
};
char fileapi2_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c',
    '\x73', '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c',
    '\x31', '\x0a', '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a',
    '\x31', '\x2c', '\x33', '\x2c', '\x33', '\x0a',
};
char fileapi3_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c', '\x73',
    '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c', '\x31', '\x0a',
    '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a', '\x31', '\x2c', '\x33',
    '\x2c', '\x33', '\x0a', '\x31', '\x2c', '\x34', '\x2c', '\x34', '\x0a',
};
char fileapi4_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c', '\x73',
    '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c', '\x31', '\x0a',
    '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a', '\x31', '\x2c', '\x33',
    '\x2c', '\x33', '\x0a', '\x31', '\x2c', '\x34', '\x2c', '\x34', '\x0a',
    '\x32', '\x2c', '\x31', '\x2c', '\x35', '\x0a',
};
char fileapi5_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c',
    '\x73', '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c',
    '\x31', '\x0a', '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a',
    '\x31', '\x2c', '\x33', '\x2c', '\x33', '\x0a', '\x31', '\x2c',
    '\x34', '\x2c', '\x34', '\x0a', '\x32', '\x2c', '\x31', '\x2c',
    '\x35', '\x0a', '\x32', '\x2c', '\x32', '\x2c', '\x36', '\x0a',
};
char fileapi6_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c', '\x73',
    '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c', '\x31', '\x0a',
    '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a', '\x31', '\x2c', '\x33',
    '\x2c', '\x33', '\x0a', '\x31', '\x2c', '\x34', '\x2c', '\x34', '\x0a',
    '\x32', '\x2c', '\x31', '\x2c', '\x35', '\x0a', '\x32', '\x2c', '\x32',
    '\x2c', '\x36', '\x0a', '\x32', '\x2c', '\x33', '\x2c', '\x37', '\x0a',
};
char fileapi7_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c', '\x73',
    '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c', '\x31', '\x0a',
    '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a', '\x31', '\x2c', '\x33',
    '\x2c', '\x33', '\x0a', '\x31', '\x2c', '\x34', '\x2c', '\x34', '\x0a',
    '\x32', '\x2c', '\x31', '\x2c', '\x35', '\x0a', '\x32', '\x2c', '\x32',
    '\x2c', '\x36', '\x0a', '\x32', '\x2c', '\x33', '\x2c', '\x37', '\x0a',
    '\x32', '\x2c', '\x34', '\x2c', '\x38', '\x0a',
};
char fileapi8_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c', '\x73',
    '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c', '\x31', '\x0a',
    '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a', '\x31', '\x2c', '\x33',
    '\x2c', '\x33', '\x0a', '\x31', '\x2c', '\x34', '\x2c', '\x34', '\x0a',
    '\x32', '\x2c', '\x31', '\x2c', '\x35', '\x0a', '\x32', '\x2c', '\x32',
    '\x2c', '\x36', '\x0a', '\x32', '\x2c', '\x33', '\x2c', '\x37', '\x0a',
    '\x32', '\x2c', '\x34', '\x2c', '\x38', '\x0a', '\x33', '\x2c', '\x31',
    '\x2c', '\x39', '\x0a',
};
char fileapi9_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c', '\x73',
    '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c', '\x31', '\x0a',
    '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a', '\x31', '\x2c', '\x33',
    '\x2c', '\x33', '\x0a', '\x31', '\x2c', '\x34', '\x2c', '\x34', '\x0a',
    '\x32', '\x2c', '\x31', '\x2c', '\x35', '\x0a', '\x32', '\x2c', '\x32',
    '\x2c', '\x36', '\x0a', '\x32', '\x2c', '\x33', '\x2c', '\x37', '\x0a',
    '\x32', '\x2c', '\x34', '\x2c', '\x38', '\x0a', '\x33', '\x2c', '\x31',
    '\x2c', '\x39', '\x0a', '\x33', '\x2c', '\x32', '\x2c', '\x31', '\x30',
    '\x0a',
};
char quickstart_dense_csv[] = {
    '\x72', '\x6f', '\x77', '\x73', '\x2c', '\x63', '\x6f', '\x6c', '\x73',
    '\x2c', '\x61', '\x0a', '\x31', '\x2c', '\x31', '\x2c', '\x31', '\x0a',
    '\x31', '\x2c', '\x32', '\x2c', '\x32', '\x0a', '\x31', '\x2c', '\x33',
    '\x2c', '\x33', '\x0a', '\x31', '\x2c', '\x34', '\x2c', '\x34', '\x0a',
    '\x32', '\x2c', '\x31', '\x2c', '\x35', '\x0a', '\x32', '\x2c', '\x32',
    '\x2c', '\x36', '\x0a', '\x32', '\x2c', '\x33', '\x2c', '\x37', '\x0a',
    '\x32', '\x2c', '\x34', '\x2c', '\x38', '\x0a', '\x33', '\x2c', '\x31',
    '\x2c', '\x39', '\x0a', '\x33', '\x2c', '\x32', '\x2c', '\x31', '\x30',
    '\x0a', '\x33', '\x2c', '\x33', '\x2c', '\x31', '\x31', '\x0a', '\x33',
    '\x2c', '\x34', '\x2c', '\x31', '\x32', '\x0a', '\x34', '\x2c', '\x31',
    '\x2c', '\x31', '\x33', '\x0a', '\x34', '\x2c', '\x32', '\x2c', '\x31',
    '\x34', '\x0a', '\x34', '\x2c', '\x33', '\x2c', '\x31', '\x35', '\x0a',
    '\x34', '\x2c', '\x34', '\x2c', '\x31', '\x36', '\x0a',
};
char quickstart_dense_csv_gz[] = {
    '\x1f', '\x8b', '\x08', '\x08', '\x96', '\x1a', '\x89', '\x61', '\x00',
    '\x03', '\x71', '\x75', '\x69', '\x63', '\x6b', '\x73', '\x74', '\x61',
    '\x72', '\x74', '\x5f', '\x64', '\x65', '\x6e', '\x73', '\x65', '\x2e',
    '\x63', '\x73', '\x76', '\x00', '\x15', '\x8c', '\x49', '\x0a', '\x80',
    '\x30', '\x10', '\x04', '\xef', '\xf3', '\x96', '\x3e', '\x38', '\x4b',
    '\xa2', '\x3e', '\x47', '\xbc', '\x0a', '\x81', '\xe4', '\xe0', '\xf7',
    '\x2d', '\x19', '\xa8', '\xa2', '\xbb', '\x61', '\xe6', '\x78', '\x97',
    '\xee', '\xf1', '\x2c', '\x5d', '\xe6', '\xe2', '\x60', '\x28', '\x60',
    '\x2a', '\x61', '\xa9', '\x2c', '\xe8', '\x1b', '\x0c', '\x75', '\x98',
    '\xda', '\x61', '\xe9', '\xb0', '\xa4', '\x3f', '\x21', '\xfb', '\x86',
    '\x88', '\x8e', '\x4a', '\x1e', '\x56', '\xff', '\xab', '\x44', '\x6c',
    '\x85', '\xd8', '\x1a', '\xa2', '\xee', '\xf6', '\x01', '\xec', '\xb3',
    '\xa7', '\xa8', '\x73', '\x00', '\x00', '\x00',
};
char text_txt[] = {
    '\x53', '\x69', '\x6d', '\x70', '\x6c', '\x65', '\x20', '\x74', '\x65',
    '\x78', '\x74', '\x20', '\x66', '\x69', '\x6c', '\x65', '\x2e', '\x0a',
    '\x57', '\x69', '\x74', '\x68', '\x20', '\x74', '\x77', '\x6f', '\x20',
    '\x6c', '\x69', '\x6e', '\x65', '\x73', '\x2e', '\x0a',
};

struct file_data_sizes_s {
  const char* file_name;
  char* file_data;
  uint64_t file_data_len;
} file_data_sizes1[] = {{"empty_text", empty_txt, strlen(empty_txt)},
                        {"fileapi0_csv", fileapi0_csv, sizeof(fileapi0_csv)},
                        {"fileapi1_csv", fileapi1_csv, sizeof(fileapi1_csv)},
                        {"fileapi2_csv", fileapi2_csv, sizeof(fileapi2_csv)},
                        {"fileapi3_csv", fileapi3_csv, sizeof(fileapi3_csv)},
                        {"fileapi4_csv", fileapi4_csv, sizeof(fileapi4_csv)},
                        {"fileapi5_csv", fileapi5_csv, sizeof(fileapi5_csv)},
                        {"fileapi6_csv", fileapi6_csv, sizeof(fileapi6_csv)},
                        {"fileapi7_csv", fileapi7_csv, sizeof(fileapi7_csv)},
                        {"fileapi8_csv", fileapi8_csv, sizeof(fileapi8_csv)},
                        {"fileapi9_csv", fileapi9_csv, sizeof(fileapi9_csv)},
                        {"quickstart_dense_csv",
                         quickstart_dense_csv,
                         sizeof(quickstart_dense_csv)},
                        {"quickstart_dense_csv_gz",
                         quickstart_dense_csv_gz,
                         sizeof(quickstart_dense_csv_gz)},
                        {"text_txt", text_txt, sizeof(text_txt)},

                        {0, 0, 0}},
  file_data_sizes2[] = {{"text_txt", text_txt, sizeof(text_txt)},
                        {"quickstart_dense_csv_gz",
                         quickstart_dense_csv_gz,
                         sizeof(quickstart_dense_csv_gz)},
                        {"quickstart_dense_csv",
                         quickstart_dense_csv,
                         sizeof(quickstart_dense_csv)},
                        {"fileapi9_csv", fileapi9_csv, sizeof(fileapi9_csv)},
                        {"fileapi8_csv", fileapi8_csv, sizeof(fileapi8_csv)},
                        {"fileapi7_csv", fileapi7_csv, sizeof(fileapi7_csv)},
                        {"fileapi6_csv", fileapi6_csv, sizeof(fileapi6_csv)},
                        {"fileapi5_csv", fileapi5_csv, sizeof(fileapi5_csv)},
                        {"fileapi4_csv", fileapi4_csv, sizeof(fileapi4_csv)},
                        {"fileapi3_csv", fileapi3_csv, sizeof(fileapi3_csv)},
                        {"fileapi2_csv", fileapi2_csv, sizeof(fileapi2_csv)},
                        {"fileapi1_csv", fileapi1_csv, sizeof(fileapi1_csv)},
                        {"fileapi0_csv", fileapi0_csv, sizeof(fileapi0_csv)},
                        {"empty_text", empty_txt, strlen(empty_txt)},

                        {0, 0, 0}};

int embedded_vs_external_identifications() {
  int errcnt = 0;

  magic_t magic_mimeenc_embedded;
  magic_t magic_mimeenc_external;
  magic_t magic_mimetyp_embedded;
  magic_t magic_mimetyp_external;

  auto magic_opens = [&]() {
    auto magic_debug_options = 0;  // MAGIC_DEBUG | MAGIC_CHECK | MAGIC_ERROR;
    magic_mimeenc_embedded =
        magic_open(MAGIC_MIME_ENCODING | magic_debug_options);
    if (auto rval = magic_dict::magic_mgc_embedded_load(magic_mimeenc_embedded);
        rval != 0) {
      auto str_rval = std::to_string(rval);
      auto err = magic_error(magic_mimeenc_embedded);
      if (!err) {
        err =
            "(magic_error() returned 0, unexpected error loading embedded "
            "data ";
      }
      magic_close(magic_mimeenc_embedded);
      std::cerr << std::string("cannot load magic database - ") + str_rval +
                       err;
      return 1;
    }
    magic_mimeenc_external =
        magic_open(MAGIC_MIME_ENCODING | magic_debug_options);
    if (auto rval =
            magic_load(magic_mimeenc_external, TILEDB_PATH_TO_MAGIC_MGC);
        rval != 0) {
      auto str_rval = std::to_string(rval);
      auto err = magic_error(magic_mimeenc_external);
      if (!err) {
        err =
            "(magic_error() returned 0, try setting env var 'MAGIC' to "
            "location "
            "of magic.mgc or "
            "alternate!)";
      }
      magic_close(magic_mimeenc_external);
      std::cerr << std::string("cannot load magic database - ") + str_rval +
                       err;
      return 2;
    }
    magic_mimetyp_embedded = magic_open(MAGIC_MIME_TYPE | magic_debug_options);
    if (auto rval = magic_dict::magic_mgc_embedded_load(magic_mimetyp_embedded);
        rval != 0) {
      auto str_rval = std::to_string(rval);
      auto err = magic_error(magic_mimetyp_embedded);
      if (!err) {
        err =
            "(magic_error() returned 0, unexpected error loading embedded "
            "data ";
      }
      magic_close(magic_mimetyp_embedded);
      std::cerr << std::string("cannot load magic database - ") + str_rval +
                       err;
      return 1;
    }
    magic_mimetyp_external = magic_open(MAGIC_MIME_TYPE | magic_debug_options);
    if (auto rval =
            magic_load(magic_mimetyp_external, TILEDB_PATH_TO_MAGIC_MGC);
        rval != 0) {
      auto str_rval = std::to_string(rval);
      auto err = magic_error(magic_mimetyp_external);
      if (!err) {
        err =
            "(magic_error() returned 0, try setting env var 'MAGIC' to "
            "location "
            "of magic.mgc or "
            "alternate!)";
      }
      magic_close(magic_mimetyp_external);
      std::cerr << std::string("cannot load magic database - ") + str_rval +
                       err;
      return 2;
    }
    return 0;
  };

  auto magic_closes = [&]() {
    magic_close(magic_mimeenc_embedded);
    magic_close(magic_mimeenc_external);
    magic_close(magic_mimetyp_embedded);
    magic_close(magic_mimetyp_external);
  };

  auto proc_list = [&](file_data_sizes_s file_data_sizes[],
                       bool global_open_close = true) {
    if (global_open_close) {
      if (auto rval = magic_opens(); rval != 0) {
        return rval;
      }
    }

    auto file_item = &file_data_sizes[0];
    while (file_item->file_data) {
      if (!global_open_close) {
        if (auto rval = magic_opens(); rval != 0) {
          return rval;
        }
      }
      const char* mime_type_embedded = nullptr;
      const char* mime_type_external = nullptr;
      const char* mime_enc_embedded = nullptr;
      const char* mime_enc_external = nullptr;

      mime_type_embedded = magic_buffer(
          magic_mimetyp_embedded,
          file_item->file_data,
          file_item->file_data_len);
      if (!mime_type_embedded) {
        auto str_rval = std::to_string(magic_errno(magic_mimeenc_embedded));
        auto err = magic_error(magic_mimeenc_embedded);
        if (!err) {
          err = "(magic_buffer(..._embedded) returned 0!)";
        }
        magic_close(magic_mimeenc_embedded);
        std::cerr << std::string("cannot access mime_type - ") + str_rval + err;
        return 2;
      }
      mime_type_external = magic_buffer(
          magic_mimetyp_external,
          file_item->file_data,
          file_item->file_data_len);
      if (!mime_type_external) {
        auto str_rval = std::to_string(magic_errno(magic_mimetyp_external));
        auto err = magic_error(magic_mimetyp_external);
        if (!err) {
          err = "(magic_buffer(..._external) returned 0!)";
        }
        magic_close(magic_mimetyp_external);
        std::cerr << std::string("cannot access mime_type - ") + str_rval + err;
        return 2;
      }

      if (strcmp(mime_type_embedded, mime_type_external)) {
        fprintf(
            stderr,
            "ERROR mismatch (%s) mime type embedded (%s) vs external (%s)\n",
            file_item->file_name,
            mime_type_embedded,
            mime_type_external);
        ++errcnt;
      }

      mime_enc_embedded = magic_buffer(
          magic_mimeenc_embedded,
          file_item->file_data,
          file_item->file_data_len);
      if (!mime_enc_embedded) {
        auto str_rval = std::to_string(magic_errno(magic_mimeenc_embedded));
        auto err = magic_error(magic_mimeenc_embedded);
        if (!err) {
          err = "(magic_buffer(..._embedded) returned 0!)";
        }
        magic_close(magic_mimeenc_embedded);
        std::cerr << std::string("cannot access mime_encoding - ") + str_rval +
                         err;
        return 2;
      }
      mime_enc_external = magic_buffer(
          magic_mimeenc_external,
          file_item->file_data,
          file_item->file_data_len);
      if (!mime_enc_embedded) {
        auto str_rval = std::to_string(magic_errno(magic_mimeenc_external));
        auto err = magic_error(magic_mimeenc_external);
        if (!err) {
          err = "(magic_buffer(..._embedded) returned 0!)";
        }
        magic_close(magic_mimeenc_external);
        std::cerr << std::string("cannot access mime_encoding - ") + str_rval +
                         err;
        return 2;
      }

      if (strcmp(mime_enc_embedded, mime_enc_external)) {
        fprintf(
            stderr,
            "ERROR mismatch (%s) mime encoding embedded (%s) vs external "
            "(%s)\n",
            file_item->file_name,
            mime_enc_embedded,
            mime_enc_external);
        ++errcnt;
      }
      file_item++;
      if (!global_open_close) {
        magic_closes();
      }
    }
    if (global_open_close) {
      magic_closes();
    }

    return 0;
  };

  if (auto rval = proc_list(file_data_sizes1, true); rval != 0)
    return rval;
  if (auto rval = proc_list(file_data_sizes2, true); rval != 0)
    return rval;

  if (auto rval = proc_list(file_data_sizes1, false); rval != 0)
    return rval;
  if (auto rval = proc_list(file_data_sizes2, false); rval != 0)
    return rval;

  if (errcnt) {
    fprintf(
        stderr, "%d mismatch errors magic embedded vs external data\n", errcnt);
  }

  return errcnt ? 99 : 0;
}

int main() {
  int failures = 0;

  failures |= check_embedded_data_validity();
  failures |= embedded_vs_external_identifications();

  if (failures) {
    fprintf(stderr, "\nERRORS mgc_dict validation\n");
    return 1;
  } else {
    fprintf(stderr, "NO errors encountered in mgc_dict validation\n");
    return 0;
  }
}
