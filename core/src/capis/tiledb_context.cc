/**
 * @file   tiledb_context.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the TileDB_Context initialization/finalization 
 * functions.
 */

#include "loader.h"
#include "query_processor.h"
#include "storage_manager.h"
#include "tiledb_context.h"
#include "tiledb_error.h"

int tiledb_finalize(TileDB_Context*& tiledb_context) {
  // Clear the TileDB modules
  delete static_cast<StorageManager*>(tiledb_context->storage_manager_);
  delete static_cast<Loader*>(tiledb_context->loader_);
  delete static_cast<QueryProcessor*>(tiledb_context->query_processor_);

  // Delete the TileDB context
  free(tiledb_context); 
  tiledb_context = NULL;

  return 0;
}

int tiledb_init(const char* workspace, TileDB_Context*& tiledb_context) {
  // Create Storage Manager
  StorageManager* storage_manager = new StorageManager(workspace);
  if(storage_manager->err()) {
    std::cerr << ERROR_MSG_HEADER << " Cannot create storage manager.\n";
    return TILEDB_ENSMCREAT;
  }

  // Create Loader
  Loader* loader = new Loader(storage_manager, workspace);
  if(loader->err()) {
    std::cerr << ERROR_MSG_HEADER << " Cannot create loader.\n";
    return TILEDB_ENLDCREAT;
  }

  // Create Query Processor
  QueryProcessor* query_processor = new QueryProcessor(storage_manager);
  if(query_processor->err()) {
    std::cerr << ERROR_MSG_HEADER << " Cannot create query processor.\n";
    return TILEDB_ENQPCREAT;
  }

  // Create a TileDB context and store the modules in it
  tiledb_context = (TileDB_Context*) malloc(sizeof(struct TileDB_Context)); 
  tiledb_context->storage_manager_ = storage_manager;
  tiledb_context->loader_ = loader;
  tiledb_context->query_processor_ = query_processor;

  return 0;
}
