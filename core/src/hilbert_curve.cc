#include <math.h>
#include "hilbert_curve.h"

void HilbertCurve::TransposetoLine(
	int* Line,         //  Hilbert integer 
	int* X,            //  Transpose      
	int  b,            //  # bits
	int  n)            //  dimension
{
	int j, p, M;
	int i, q;

    	M = 1 << (b - 1);
    	q = 0;    p = M;
    	for( i = 0; i < n; i++ )
    	{
        	Line[i] = 0;
        	for( j = M; j; j >>= 1 )
        	{
            		if( X[q] & p )
                		Line[i] |= j;
            		if( ++q == n )
        		{
	                	q = 0;    
				p >>= 1;
            		}
        	}
    	}
}


int64_t HilbertCurve::AxestoLine(
	int* Axes,    //    multidimensional geometrical axes  
	int b,        //    # bits used in each word
	int n)        //    dimension
{
	int store[1024];   // avoid overwriting Axes
	int i;             // counter
    	int Line[n];


    	if( n <= 1 )            // trivial case
        	*Line = *Axes;
    	else if( n <= 1024 )    // surely the usual case
    	{
        	for( i = 0; i < n; ++i )
           		store[i] = Axes[i];
        	AxestoTranspose(store, b, n);
        	TransposetoLine(Line, store, b, n);
    	}
    	else                    // must do in place at greater cost
    	{
        	AxestoTranspose(Axes, b, n);
        	TransposetoLine(Line, Axes, b, n);
        	TransposetoAxes(Axes, b, n);
    	}

    	int64_t result = 0;
    	for(i = 0; i < n; i++)
		result+=Line[i]*pow(2,(b*(n-i-1)));

    	return result;
}


void HilbertCurve::LinetoAxes(
	int* Axes,    //    multidimensional geometrical axes  
	int* Line,    //    linear serial number, stored as     
	int b,        //    # bits used in each word
	int n)        //    dimension
{
	if( n <= 1 )            // trivial case
        	*Axes = *Line;
    	else
    	{
        	LinetoTranspose(Axes, Line, b, n);
        	TransposetoAxes(Axes,       b, n);
    	}
}



void HilbertCurve::TransposetoAxes(
	int* X,      //   position  
	int b,       //   # bits
	int n)       //   dimension
{
	int M, P, Q, t;
    	int i;

	// Gray decode by  H ^ (H/2)
    	t = X[n-1] >> 1;
    	for( i = n-1; i; i-- )
        	X[i] ^= X[i-1];
    	X[0] ^= t;

	// Undo excess work
    	M = 2 << (b - 1);
    	for( Q = 2; Q != M; Q <<= 1 )
    	{
        	P = Q - 1;
        	for( i = n-1; i; i-- )
            		if( X[i] & Q ) X[0] ^= P;                              // invert
            		else{ t = (X[0] ^ X[i]) & P;  X[0] ^= t;  X[i] ^= t; } // exchange
        	if( X[0] & Q ) X[0] ^= P;                                  // invert
    	}
} 



void HilbertCurve::AxestoTranspose(
	int* X,   //  position 
	int b,    //  # bits
	int n)    //  dimension
{
	int P, Q, t;
    	int i;

	// Inverse undo
    	for( Q = 1 << (b - 1); Q > 1; Q >>= 1 )
    	{
        	P = Q - 1;
        	if( X[0] & Q ) X[0] ^= P;                                  // invert
        	for( i = 1; i < n; i++ )
            		if( X[i] & Q ) X[0] ^= P;                              // invert
            		else{ t = (X[0] ^ X[i]) & P;  X[0] ^= t;  X[i] ^= t; } // exchange
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




void HilbertCurve::LinetoTranspose(
	int* X,     //   Transpose        
	int* Line,  //   Hilbert integer  
	int b,      //   # bits
	int n)      //   dimension
{
	int j, p, M;
	int i, q;

	M = 1 << (b - 1);
	for( i = 0; i < n; i++ )
		X[i] = 0;
    	q = 0;    
	p = M;
    	for( i = 0; i < n; i++ )
    	{
        	for( j = M; j; j >>= 1 )
        	{
            		if( Line[i] & j )
                		X[q] |= p;
            		if( ++q == n )
            		{
                		q = 0;    
				p >>= 1;
            		}
        	}	
    	}
}
