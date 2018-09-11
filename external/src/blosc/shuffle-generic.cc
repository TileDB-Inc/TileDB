/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSES/BLOSC.txt for details about copyright and rights to use.

  Modifications for TileDB by Tyler Denniston <tyler@tiledb.io>
**********************************************************************/

#include "shuffle-generic.h"

namespace blosc {

void shuffle_generic_inline(const size_t type_size,
                            const size_t vectorizable_blocksize, const size_t blocksize,
                            const uint8_t* const _src, uint8_t* const _dest)
{
  size_t i, j;
  /* Calculate the number of elements in the block. */
  const size_t neblock_quot = blocksize / type_size;
  const size_t neblock_rem = blocksize % type_size;
  const size_t vectorizable_elements = vectorizable_blocksize / type_size;


  /* Non-optimized shuffle */
  for (j = 0; j < type_size; j++) {
    for (i = vectorizable_elements; i < (size_t)neblock_quot; i++) {
      _dest[j*neblock_quot+i] = _src[i*type_size+j];
    }
  }

  /* Copy any leftover bytes in the block without shuffling them. */
  memcpy(_dest + (blocksize - neblock_rem), _src + (blocksize - neblock_rem), neblock_rem);
}

void unshuffle_generic_inline(const size_t type_size,
                              const size_t vectorizable_blocksize, const size_t blocksize,
                              const uint8_t* const _src, uint8_t* const _dest)
{
  size_t i, j;

  /* Calculate the number of elements in the block. */
  const size_t neblock_quot = blocksize / type_size;
  const size_t neblock_rem = blocksize % type_size;
  const size_t vectorizable_elements = vectorizable_blocksize / type_size;

  /* Non-optimized unshuffle */
  for (i = vectorizable_elements; i < (size_t)neblock_quot; i++) {
    for (j = 0; j < type_size; j++) {
      _dest[i*type_size+j] = _src[j*neblock_quot+i];
    }
  }

  /* Copy any leftover bytes in the block without unshuffling them. */
  memcpy(_dest + (blocksize - neblock_rem), _src + (blocksize - neblock_rem), neblock_rem);
}

/* Shuffle a block.  This can never fail. */
void shuffle_generic(const size_t bytesoftype, const size_t blocksize,
		     const uint8_t* const _src, uint8_t* const _dest)
{
  /* Non-optimized shuffle */
  shuffle_generic_inline(bytesoftype, 0, blocksize, _src, _dest);
}

/* Unshuffle a block.  This can never fail. */
void unshuffle_generic(const size_t bytesoftype, const size_t blocksize,
                       const uint8_t* const _src, uint8_t* const _dest)
{
  /* Non-optimized unshuffle */
  unshuffle_generic_inline(bytesoftype, 0, blocksize, _src, _dest);
}

}