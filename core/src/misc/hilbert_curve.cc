#include "hilbert_curve.h"
#include <assert.h>
#include <string.h>

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

HilbertCurve::HilbertCurve(int bits, int dim_num) 
    : bits_(bits), dim_num_(dim_num) {
  assert(dim_num >=0 && dim_num < HC_MAX_DIM);
  assert(bits * dim_num <= sizeof(int64_t)*8);
}

HilbertCurve::~HilbertCurve() {
}

/******************************************************
******************* MAIN FUNCTIONS ********************
******************************************************/

void HilbertCurve::coords_to_hilbert(const int* coords, int64_t& hilbert) {
  // Copy coords to temporary storage
  memcpy(temp_, coords, dim_num_ * sizeof(int));

  // Convert coords to the transpose form of the hilbert value
  AxestoTranspose(temp_, bits_, dim_num_);

  // Convert the hilbert transpose form into an int64_t hilbert value
  hilbert = 0; 
  int64_t c = 1; // This is a bit shifted from right to left over temp_[i]
  int64_t h = 1; // This is a bit shifted from right to left over hilbert
  for(int j=0; j<bits_; ++j, c <<= 1) {
    for(int i=dim_num_-1; i>=0; --i, h <<= 1) {
      if(temp_[i] & c)
        hilbert |= h; 
    } 
  }
}

void HilbertCurve::hilbert_to_coords(int64_t hilbert, int* coords) {
  // Initialization
  for(int i=0; i<dim_num_; ++i) 
    temp_[i] = 0;

  // Convert the int64_t hilbert value to its transpose form
  int64_t c = 1; // This is a bit shifted from right to left over temp_[i]
  int64_t h = 1; // This is a bit shifted from right to left over hilbert
  for(int j=0; j<bits_; ++j, c <<= 1) {
    for(int i=dim_num_-1; i>=0; --i, h <<= 1) {
      if(hilbert & h)
        temp_[i] |= c; 
    } 
  }

  // Convert coords to the transpose form of the hilbert value
  TransposetoAxes(temp_, bits_, dim_num_);

  // Copy from the temporary storage to the (output) coords
  memcpy(coords, temp_, dim_num_ * sizeof(int));
}

/******************************************************
******************* MAIN FUNCTIONS ********************
******************************************************/

void HilbertCurve::AxestoTranspose(int* X, int b, int n) {
  int P, Q, t, i;

  // Inverse undo
  for( Q = 1 << (b - 1); Q > 1; Q >>= 1 ) {
    P = Q - 1;
    if( X[0] & Q )      // invert
      X[0] ^= P;                                 
    for( i = 1; i < n; i++ ) 
      if( X[i] & Q )    // invert
        X[0] ^= P;                              
      else {            // exchange
        t = (X[0] ^ X[i]) & P;  
        X[0] ^= t;  
        X[i] ^= t; 
      } 
  }

  // Gray encode (inverse of decode)
  for( i = 1; i < n; i++ )
    X[i] ^= X[i-1];
  t = X[n-1];
  for( i = 1; i < b; i <<= 1 )
    X[n-1] ^= X[n-1] >> i;
  t ^= X[n-1];
  for( i = n-2; i >= 0; i-- )
    X[i] ^= t;
}

void HilbertCurve::TransposetoAxes(int* X, int b, int n) {
  int M, P, Q, t, i;

  // Gray decode by H ^ (H/2)
  t = X[n-1] >> 1;
  for( i = n-1; i; i-- )
    X[i] ^= X[i-1];
  X[0] ^= t;

  // Undo excess work
  M = 2 << (b - 1);
  for( Q = 2; Q != M; Q <<= 1 ) {
    P = Q - 1;
    for( i = n-1; i; i-- )
      if( X[i] & Q )  // invert
        X[0] ^= P;                              
      else {          // exchange
        t = (X[0] ^ X[i]) & P;  
        X[0] ^= t;  
        X[i] ^= t; 
      } 
      if( X[0] & Q )  // invert
        X[0] ^= P; 
  }
} 

