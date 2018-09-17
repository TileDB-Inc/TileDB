/*********************************************************************
  Blosc (v1.14.4) - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSES/BLOSC.txt for details about copyright and rights to use.

  Modifications for TileDB by Tyler Denniston <tyler@tiledb.io>
**********************************************************************/

/* Generic (non-hardware-accelerated) shuffle/unshuffle routines.
   These are used when hardware-accelerated functions aren't available
   for a particular platform; they are also used by the hardware-
   accelerated functions to handle any remaining elements in a block
   which isn't a multiple of the hardware's vector size. */

#ifndef SHUFFLE_GENERIC_H
#define SHUFFLE_GENERIC_H

#include "blosc-common.h"
#include <stdlib.h>

namespace blosc {

/**
  Generic (non-hardware-accelerated) shuffle routine.
  This is the pure element-copying nested loop. It is used by the
  generic shuffle implementation and also by the vectorized shuffle
  implementations to process any remaining elements in a block which
  is not a multiple of (type_size * vector_size).
*/
void shuffle_generic_inline(const size_t type_size,
    const size_t vectorizable_blocksize, const size_t blocksize,
    const uint8_t* const _src, uint8_t* const _dest);

/**
  Generic (non-hardware-accelerated) unshuffle routine.
  This is the pure element-copying nested loop. It is used by the
  generic unshuffle implementation and also by the vectorized unshuffle
  implementations to process any remaining elements in a block which
  is not a multiple of (type_size * vector_size).
*/
void unshuffle_generic_inline(const size_t type_size,
  const size_t vectorizable_blocksize, const size_t blocksize,
  const uint8_t* const _src, uint8_t* const _dest);

/**
  Generic (non-hardware-accelerated) shuffle routine.
*/
BLOSC_NO_EXPORT void shuffle_generic(const size_t bytesoftype, const size_t blocksize,
                                      const uint8_t* const _src, uint8_t* const _dest);

/**
  Generic (non-hardware-accelerated) unshuffle routine.
*/
BLOSC_NO_EXPORT void unshuffle_generic(const size_t bytesoftype, const size_t blocksize,
                                        const uint8_t* const _src, uint8_t* const _dest);

}

#endif /* SHUFFLE_GENERIC_H */
