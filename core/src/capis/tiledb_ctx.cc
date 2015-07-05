/**
 * @file   tiledb_ctx.cc
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
 * This file implements the TileDB_CTX initialization/finalization 
 * functions.
 */

#include "loader.h"
#include "query_processor.h"
#include "storage_manager.h"
#include "tiledb_ctx.h"
#include "tiledb_error.h"

typedef struct TileDB_CTX{
  Loader* loader_;
  QueryProcessor* query_processor_;
  StorageManager* storage_manager_;
} TileDB_CTX;

int tiledb_finalize(TileDB_CTX*& tiledb_ctx) {
  // Clear the TileDB modules
  delete tiledb_ctx->storage_manager_;
  delete tiledb_ctx->loader_;
  delete tiledb_ctx->query_processor_;

  // Delete the TileDB context
  free(tiledb_ctx); 
  tiledb_ctx = NULL;

  return 0;
}

int tiledb_init(const char* workspace, TileDB_CTX*& tiledb_ctx) {
  tiledb_ctx = (TileDB_CTX*) malloc(sizeof(struct TileDB_CTX)); 

  // Create Storage Manager
  tiledb_ctx->storage_manager_ = new StorageManager(workspace);
  if(tiledb_ctx->storage_manager_->err()) {
    delete tiledb_ctx->storage_manager_;
    free(tiledb_ctx);
    std::cerr << ERROR_MSG_HEADER << " Cannot create storage manager.\n";
    return TILEDB_ENSMCREAT;
  }

  // Create Loader
  tiledb_ctx->loader_ = 
      new Loader(tiledb_ctx->storage_manager_, workspace);
  if(tiledb_ctx->loader_->err()) {
    delete tiledb_ctx->storage_manager_;
    delete tiledb_ctx->loader_;
    free(tiledb_ctx);
    std::cerr << ERROR_MSG_HEADER << " Cannot create loader.\n";
    return TILEDB_ENLDCREAT;
  }

  // Create Query Processor
  tiledb_ctx->query_processor_ = 
      new QueryProcessor(tiledb_ctx->storage_manager_);
  if(tiledb_ctx->query_processor_->err()) {
    delete tiledb_ctx->storage_manager_;
    delete tiledb_ctx->loader_;
    delete tiledb_ctx->query_processor_;
    free(tiledb_ctx);
    std::cerr << ERROR_MSG_HEADER << " Cannot create query processor.\n";
    return TILEDB_ENQPCREAT;
  }

  return 0;
}
