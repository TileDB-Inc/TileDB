#ifndef TILEDB_BASE_COMPRESSOR_H
#define TILEDB_BASE_COMPRESSOR_H

namespace tiledb {

class BaseCompressor {
 public:
  BaseCompressor(int level)
      : level_(level){};

  ~BaseCompressor() = default;

  int level() {
    return level_;
  }

  virtual void compress();

  virtual void decompress();

 private:
  int level_;
};

};      // namespace tiledb
#endif  // TILEDB_BASE_COMPRESSOR_H
