/**
 * @file   query.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * CLI tool to query an object.
 */

#include "tiledb/query.h"
#include "clipp/clipp.h"
#include "tiledb/array.h"
#include "tiledb/map.h"

#include <set>
#include <string>
#include <unordered_map>

void handle_array(
    tiledb::Context& ctx,
    const std::string& uri,
    const std::vector<std::string>& dim_names,
    const std::vector<std::string>& dim_start,
    const std::vector<std::string>& dim_stop,
    const std::string& attr,
    const std::string& order,
    const std::string& split,
    bool write);

int main(int argc, char* argv[]) {
  using namespace clipp;

  bool help = false, write = false, dimset = false, keyset = false;
  std::string uri, delim = ",", order = "row", attr;
  std::vector<std::string> dims, keys;

  auto array_opt = repeatable(
      option("-d", "--dim")
          .set(dimset)
          .doc("List of dimension ranges to query, coordinate inclusive.") &
      value("dim=start:stop", dims));
  auto map_opt = repeatable(
      option("-k", "--key").set(keyset).doc("Keys to query.") & value("key"));
  auto attr_opt = repeatable(
      option("-a", "--attr").doc("Attribute to query.") & value("attr", attr));
  auto order_opt =
      option("-o", "--order").doc("Query cell order. Default row.") &
      value("row, col, global", order);
  auto split_opt = option("-s", "--split")
                       .doc(
                           "Delimiter between multi-value attributes and "
                           "multi-attribute cells, default ','.") &
                   value("delim", delim);
  auto write_opt = option("-w", "--write")
                       .doc("Read from stdin, and write to subarray.")
                       .set(write);

  auto cli = (value("URI", uri).doc("Array/Map URI"),
              attr_opt,
              order_opt,
              split_opt,
              write_opt,
              array_opt | map_opt) |
             option("--help").set(help);

  auto result = parse(argc, argv, cli);

  if (!result) {
    std::cerr << "Error parsing arguments:\n";
    auto doc_label = [](const parameter& p) {
      if (!p.flags().empty())
        return p.flags().front();
      if (!p.label().empty())
        return p.label();
      return doc_string{"<?>"};
    };

    std::cerr << "args -> parameter mapping:\n";
    for (const auto& m : result) {
      std::cerr << "\t#" << m.index() << " " << m.arg() << " -> ";
      auto p = m.param();
      if (p) {
        std::cerr << doc_label(*p) << " \t";
        if (m.repeat() > 0) {
          std::cerr << (m.bad_repeat() ? "[bad repeat " : "[repeat ")
                    << m.repeat() << "]";
        }
        if (m.blocked())
          std::cerr << " [blocked]";
        if (m.conflict())
          std::cerr << " [conflict]";
        std::cerr << '\n';
      } else {
        std::cerr << " [unmapped]\n";
      }
    }

    std::cerr << "missing parameters:\n";
    for (const auto& m : result.missing()) {
      auto p = m.param();
      if (p) {
        std::cerr << doc_label(*p) << " \t";
        std::cerr << " [missing after " << m.after_index() << "]\n";
      }
    }

    exit(1);
  }

  if (help) {
    std::cout << make_man_page(cli, "tiledb-query");
    return 0;
  }

  if (delim.size() != 1)
    throw std::invalid_argument("Delimiter should be a single char.");

  std::vector<std::string> dim_names, dim_start, dim_stop;
  for (auto& d : dims) {
    auto n = d.find('=');
    auto r = d.find(':');
    if (n == std::string::npos || r == std::string::npos)
      throw std::invalid_argument(
          "Invalid dim format, expected dim_name=start:stop");
    dim_names.push_back(d.substr(0, n));
    dim_start.push_back(d.substr(n + 1, r));
    dim_stop.push_back(d.substr(r + 1));
  }

  tiledb::Context ctx;
  auto obj = tiledb::Object::object(ctx, uri);
  if (obj.type() == tiledb::Object::Type::Array) {
    if (keys.size())
      throw std::invalid_argument("Cannot query an array with a key.");
    if (!dim_names.size())
      throw std::invalid_argument("--dim must be defined for array queries.");
    handle_array(
        ctx, uri, dim_names, dim_start, dim_stop, attr, order, delim, write);
  } else if (obj.type() == tiledb::Object::Type::KeyValue) {
    if (!keys.size())
      throw std::invalid_argument("Maps must be queried with keys.");
    if (dim_names.size())
      throw std::invalid_argument("--dim is not a valid option for maps.");
  } else {
    throw std::invalid_argument("Provided URI is not a valid Array or Map.");
  }

  return 0;
}

template <typename T>
typename std::enable_if<std::is_floating_point<T>::value>::type from_string(
    const std::string& s, T& ret) {
  ret = std::stod(s);
}

template <typename T>
typename std::enable_if<
    std::is_integral<T>::value && std::is_signed<T>::value>::type
from_string(const std::string& s, T& ret) {
  ret = std::stoll(s);
}

template <typename T>
typename std::enable_if<
    std::is_integral<T>::value && std::is_unsigned<T>::value>::type
from_string(const std::string& s, T& ret) {
  ret = std::stoull(s);
}

tiledb_layout_t layout(const std::string& s) {
  if (s == "row")
    return TILEDB_ROW_MAJOR;
  if (s == "col")
    return TILEDB_COL_MAJOR;
  if (s == "global")
    return TILEDB_GLOBAL_ORDER;
  throw std::invalid_argument(
      "Invalid layout, expected row, col, or global: " + s);
}

template <typename D, typename A>
void handle_array(
    tiledb::Context& ctx,
    const std::string& uri,
    const std::vector<std::string>& dim_names,
    const std::vector<std::string>& dim_start,
    const std::vector<std::string>& dim_stop,
    const std::string& attr,
    const std::string& order,
    const std::string& split,
    bool write) {
  tiledb::Query query(ctx, uri, write ? TILEDB_WRITE : TILEDB_READ);
  tiledb::ArraySchema schema(ctx, uri);

  auto dims = schema.domain().dimensions();
  if (dims.size() != dim_names.size())
    throw std::invalid_argument("All dimensions are not defined.");
  for (auto& d : dims) {
    if (std::find(dim_names.begin(), dim_names.end(), d.name()) ==
        dim_names.end()) {
      throw std::invalid_argument("Dim " + d.name() + " is not defined.");
    }
  }

  std::vector<D> subarray;
  subarray.reserve(dim_names.size() * 2);
  for (size_t i = 0; i < dim_names.size(); ++i) {
    D val;
    from_string(dim_start[i], val);
    subarray.push_back(val);
    from_string(dim_stop[i], val);
    subarray.push_back(val);
  }

  query.set_layout(layout(order));

  if (!write) {
    auto buff_el = tiledb::Array::max_buffer_elements(uri, schema, subarray);
    if (buff_el.count(attr) == 0)
      throw std::invalid_argument("Array does not have attribute " + attr);

    std::vector<A> buff(buff_el[attr].second);
    std::vector<uint64_t> offsets(buff_el[attr].first);
    if (buff_el[attr].first == 0) {
      // Fixed sized attr
      query.set_buffer(attr, buff);
    } else {
      query.set_buffer(attr, offsets, buff);
    }

    if (query.submit() != tiledb::Query::Status::COMPLETE)
      throw std::runtime_error("Error completing query.");

    for (size_t i = 0; i < buff.size(); ++i) {
      std::cout << buff[i];
      if (i != buff.size() - 1) {
        std::cout << split;
      }
    }
  }
}

template <typename D>
void handle_array(
    tiledb::Context& ctx,
    const std::string& uri,
    const std::vector<std::string>& dim_names,
    const std::vector<std::string>& dim_start,
    const std::vector<std::string>& dim_stop,
    const std::string& attr,
    const std::string& order,
    const std::string& split,
    bool write) {
  tiledb::Attribute tiledb_attr = tiledb::ArraySchema(ctx, uri).attribute(attr);

  switch (tiledb_attr.type()) {
    case TILEDB_INT32:
      handle_array<D, int32_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_INT64:
      handle_array<D, int64_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_FLOAT32:
      handle_array<D, float>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_FLOAT64:
      handle_array<D, double>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_INT8:
      handle_array<D, int8_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_UINT8:
      handle_array<D, uint8_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_INT16:
      handle_array<D, int16_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_UINT16:
      handle_array<D, uint16_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_UINT32:
      handle_array<D, uint32_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_UINT64:
      handle_array<D, uint64_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    default:
      handle_array<D, char>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
  }
};

void handle_array(
    tiledb::Context& ctx,
    const std::string& uri,
    const std::vector<std::string>& dim_names,
    const std::vector<std::string>& dim_start,
    const std::vector<std::string>& dim_stop,
    const std::string& attr,
    const std::string& order,
    const std::string& split,
    bool write) {
  tiledb::Domain domain = tiledb::ArraySchema(ctx, uri).domain();

  switch (domain.type()) {
    case TILEDB_INT32:
      handle_array<int32_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_INT64:
      handle_array<int64_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_FLOAT32:
      handle_array<float>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_FLOAT64:
      handle_array<double>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_CHAR:
      handle_array<char>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_INT8:
      handle_array<int8_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_UINT8:
      handle_array<uint8_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_INT16:
      handle_array<int16_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_UINT16:
      handle_array<uint16_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_UINT32:
      handle_array<uint32_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    case TILEDB_UINT64:
      handle_array<uint64_t>(
          ctx, uri, dim_names, dim_start, dim_stop, attr, order, split, write);
      break;
    default:
      throw std::invalid_argument("Error handling domain type.");
  }
}