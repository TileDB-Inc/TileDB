/*********************************************************************
  Blosc (v1.14.4) - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSES/BLOSC.txt for details about copyright and rights to use.

  Modifications for TileDB by Tyler Denniston <tyler@tiledb.io>
**********************************************************************/

/* SSE2-accelerated shuffle/unshuffle routines. */
   
#ifndef SHUFFLE_SSE2_H
#define SHUFFLE_SSE2_H

#include "blosc-common.h"

#ifdef __SSE2__

namespace blosc {

/**
  SSE2-accelerated shuffle routine.
*/
BLOSC_NO_EXPORT void shuffle_sse2(const size_t bytesoftype, const size_t blocksize,
                                   const uint8_t* const _src, uint8_t* const _dest);

/**
  SSE2-accelerated unshuffle routine.
*/
BLOSC_NO_EXPORT void unshuffle_sse2(const size_t bytesoftype, const size_t blocksize,
                                     const uint8_t* const _src, uint8_t* const _dest);

}

#endif

#endif /* SHUFFLE_SSE2_H */
