/*********************************************************************
  Blosc (v1.14.4) - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSES/BLOSC.txt for details about copyright and rights to use.

  Modifications for TileDB by Tyler Denniston <tyler@tiledb.io>
**********************************************************************/

/* AVX2-accelerated shuffle/unshuffle routines. */

#ifndef SHUFFLE_AVX2_H
#define SHUFFLE_AVX2_H

#include "blosc-common.h"

#ifdef __AVX2__

namespace blosc {

/**
  AVX2-accelerated shuffle routine.
*/
BLOSC_NO_EXPORT void shuffle_avx2(const size_t bytesoftype, const size_t blocksize,
                                   const uint8_t* const _src, uint8_t* const _dest);

/**
  AVX2-accelerated unshuffle routine.
*/
BLOSC_NO_EXPORT void unshuffle_avx2(const size_t bytesoftype, const size_t blocksize,
                                     const uint8_t* const _src, uint8_t* const _dest);

}

#endif

#endif /* SHUFFLE_AVX2_H */
