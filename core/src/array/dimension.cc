/**
 * @file   dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements class Dimension.
 */

#include "dimension.h"
#include <cassert>
#include <cstdlib>
#include <iostream>
#include "utils.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Dimension::Dimension(
    const char* name,
    Datatype type,
    const void* domain,
    const void* tile_extent) {
  // Set name
  if (name != nullptr)
    name_ = name;

  // Set type
  type_ = type;

  // Get type size
  uint64_t type_size = utils::datatype_size(type);

  // Set domain
  if (domain == nullptr) {
    domain_ = nullptr;
  } else {
    uint64_t domain_size = 2 * type_size;
    domain_ = malloc(domain_size);
    memcpy(domain_, domain, domain_size);
  }

  // Set tile extent
  if (tile_extent == nullptr) {
    tile_extent_ = nullptr;
  } else {
    tile_extent_ = malloc(type_size);
    memcpy(tile_extent_, tile_extent, type_size);
  }

  // Set default compressor and compression level
  compressor_ = Compressor::NO_COMPRESSION;
  compression_level_ = -1;
}

Dimension::Dimension(const Dimension* dim) {
  assert(dim != nullptr);

  name_ = dim->name();
  type_ = dim->type();
  compressor_ = dim->compressor();
  compression_level_ = dim->compression_level();

  uint64_t type_size = utils::datatype_size(type_);

  domain_ = malloc(2 * type_size);
  memcpy(domain_, dim->domain(), 2 * type_size);

  const void* tile_extent = dim->tile_extent();
  if (tile_extent == nullptr) {
    tile_extent_ = nullptr;
  } else {
    tile_extent_ = malloc(type_size);
    memcpy(tile_extent_, tile_extent, type_size);
  }
}

Dimension::~Dimension() {
  // Clean up
  if (domain_ != nullptr)
    free(domain_);
  if (tile_extent_ != nullptr)
    free(tile_extent_);
}

/* ********************************* */
/*              GETTERS              */
/* ********************************* */

Compressor Dimension::compressor() const {
  return compressor_;
}

int Dimension::compression_level() const {
  return compression_level_;
}

void* Dimension::domain() const {
  return domain_;
}

void Dimension::dump(FILE* out) const {
  // Retrieve type and compressor strings
  const char* type_s = utils::datatype_str(type_);
  const char* compressor_s = utils::compressor_str(compressor_);

  // Retrieve domain and tile extent strings
  std::string domain_s = utils::domain_str(domain_, type_);
  std::string tile_extent_s = utils::tile_extent_str(tile_extent_, type_);

  // Dump
  fprintf(out, "### Dimension ###\n");
  fprintf(out, "- Name: %s\n", name_.c_str());
  fprintf(out, "- Type: %s\n", type_s);
  fprintf(out, "- Compressor: %s\n", compressor_s);
  fprintf(out, "- Compression level: %d\n", compression_level_);
  fprintf(out, "- Domain: %s\n", domain_s.c_str());
  fprintf(out, "- Tile extent: %s\n", tile_extent_s.c_str());
}

const std::string& Dimension::name() const {
  return name_;
}

void* Dimension::tile_extent() const {
  return tile_extent_;
}

Datatype Dimension::type() const {
  return type_;
}

/* ********************************* */
/*              SETTERS              */
/* ********************************* */

void Dimension::set_compressor(Compressor compressor) {
  compressor_ = compressor;
}

void Dimension::set_compression_level(int compression_level) {
  compression_level_ = compression_level;
}

}  // namespace tiledb
