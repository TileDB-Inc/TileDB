/**
 * @file   quickstart_sparse.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * When run, this program will create a simple 2D sparse array, write some data
 * to it, and read a slice of the data back.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("/home/shaun/Documents/Arrays/vcfdata/test_array/");

/* Output from completed query:
'pos' found 0, 11416289 results.
'contig' found 11416289, 45665156 results.
'allele' found 11416289, 19661541 results.
'ac' found 0, 11416289 results.

 There is a `TileDB/valgrind.xml` on this branch. It can be read as-is or loaded
 into CLion with Run->Import Valgrind XML Results
*/

int main() {
  Config config;
  Context ctx(config);
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED);

  // For convenience, based on output from a completed query.
  uint64_t fixed_expected = 11416289;
  uint64_t contig_expected = 45665156;
  uint64_t allele_expected = 19661541;

  // Query returns INCOMPLETE status with this size (expected)
  //    uint64_t buffer_size = 1024 * 1024 * 86;

  // I've seen a few different errors with this buffer_size.
  // corrupted double-linked list
  // double free or corruption (!prev)
  // corrupted size vs. prev_size while consolidating
  uint64_t buffer_size = 1024 * 1024 * 87;
  // 91226112 / 8 = 11403264; Not large enough for (11416289) offsets

  // Completes normally
//  uint64_t buffer_size = 1024 * 1024 * 88;
//   92274688 / 8 = 11534336; Large enough for all data

  // Dimensions
  std::string contig_data(buffer_size, '0');
  std::vector<uint64_t> contig_offsets(buffer_size / sizeof(uint64_t));

  // I have not ran into issues with fixed-size data members.
  std::vector<uint32_t> pos_data(buffer_size / sizeof(uint32_t));
  query.set_data_buffer("pos", pos_data);
  query.set_data_buffer("contig", contig_data);
  query.set_offsets_buffer("contig", contig_offsets);

  // Attributes
  std::string alleles_data(buffer_size, '0');

//  std::vector<uint64_t> alleles_offsets(11403264);
  std::vector<uint64_t> alleles_offsets(buffer_size / sizeof(uint64_t));

  // I have not ran into issues with fixed-size data members.
  std::vector<int32_t> ac_data(buffer_size / sizeof(int32_t));
  query.set_data_buffer("ac", ac_data);
  query.set_data_buffer("allele", alleles_data);
  query.set_offsets_buffer("allele", alleles_offsets);

  Subarray subarray(ctx, array);
  subarray.add_range("contig", std::string("chr9"), std::string("chr9"));
  subarray.add_range("contig", std::string("chr8"), std::string("chr8"));
  subarray.add_range("contig", std::string("chr8"), std::string("chr8"));
  subarray.add_range("pos", (uint32_t)135494432, (uint32_t)136546047);
  subarray.add_range("pos", (uint32_t)11676958, (uint32_t)11760001);
  subarray.add_range("pos", (uint32_t)60678739, (uint32_t)60868027);
  query.set_subarray(subarray);

//  for (const auto& name : {"pos", "contig", "ac", "allele"}) {
//    if (!strcmp(name, "contig") || !strcmp(name, "allele")) {
//      auto est = query.est_result_size_var(name);
//      std::cout << "Estimated var-sized result for '" << name << "': " <<
//          est[0]
//                << ", " << est[1] << std::endl;
//    } else {
//      std::cout << "Estimated result size for '" << name
//                << "': " << query.est_result_size(name) << std::endl;
//    }
//  }

  auto st = query.submit();
  if (st != Query::Status::COMPLETE) {
    const char* s;
    tiledb_query_status_to_str((tiledb_query_status_t)query.query_status(), &s);
    std::cout << "Query failed: " << s << std::endl;
  } else {
    auto results = query.result_buffer_elements();
    for (const auto& result : results) {
      std::cout << "'" << result.first << "' found " << result.second.first
                << ", " << result.second.second << " results.\n";
    }
  }
}
