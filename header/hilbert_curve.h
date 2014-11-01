#ifndef HilbertCurve_class
#define HilbertCurve_class

#include <iostream>
#include <sys/types.h>
#include <cstdlib>


//=============================================================================
//              Hilbert-curve (a space-filling Peano curve) library
//=============================================================================
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Functions: LinetoAxes
//            AxestoLine
//
// Purpose:   Serial Hilbert length  <---->   multidimensional Axes position.
//
//   Space  = n-dimensional hypercube of side R = 2^b
//            Number of cells = N = R^n = 2^(n*b)
//
//   Line   = serial number of cell along Hilbert curve through hypercube
//          = extended integer of n*b bits ranging from 0 to N-1,
//            stored as vector of n unsigned b-bit integers with [0] high.
//
//
//  A composite-integer is a multi-word unsigned integer "Label" stored
//  "big endian" in N conventional unsigned integers with [0] high.
//        ___________________________________________________
//       |            |            |            |            |
//       |  Label[0]  |  Label[1]  |    ....    | Label[N-1] |
//       |____________|____________|____________|____________|
//            high                                   low
//
//   Axes   = Geometrical position of cell
//          = n b-bit integers representing coordinates.
//
// Example:   side R = 16, dimension n = 2, number of cells = N = 256.
//            Line = 9, stored in base-16 words as
//                   Line[0] = 0 (high),   Line[1] = 9 (low),
//            corresponds to position (2,3) as in diagram, stored as
//                   Axes[0] = 2,   Axes[1] = 3.
// 
//        |
//     15 |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
//        |    |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |
//        |    @   @---@   @   @   @---@   @   @   @---@   @   @   @---@   @
//        |    |           |   |           |   |           |   |           |
//        |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
//        |        |   |           |   |           |   |           |   |    
//        |    @---@   @---@---@---@   @---@   @---@   @---@---@---@   @---@
//        |    |                           |   |                           |
//        |    @   @---@---@   @---@---@   @   @   @---@---@   @---@---@   @
//        |    |   |       |   |       |   |   |   |       |   |       |   |
// Axes[1]|    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
//        |            |           |                   |           |        
//        |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
//        |    |   |       |   |       |   |   |   |       |   |       |   |
//        |    @   @---@---@   @---@---@   @---@   @---@---@   @---@---@   @
//        |    |                                                           |
//        |    @---@   @---@---@   @---@---@   @---@---@   @---@---@   @---@
//        |        |   |       |   |       |   |       |   |       |   |    
//        |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
//        |    |           |           |           |           |           |
//        |    @   @---@   @   @---@   @---@   @---@   @---@   @   @---@   @
//        |    |   |   |   |   |   |       |   |       |   |   |   |   |   |
//        |    @---@   @---@   @   @---@---@   @---@---@   @   @---@   @---@
//        |                    |                           |                
//      3 |    5---6   9---@   @   @---@---@   @---@---@   @   @---@   @---@
//        |    |   |   |   |   |   |       |   |       |   |   |   |   |   |
//      2 |    4   7---8   @   @---@   @---@   @---@   @---@   @   @---@   @
//        |    |           |           |           |           |           |
//      1 |    3---2   @---@   @---@   @---@   @---@   @---@   @---@   @---@
//        |        |   |       |   |       |   |       |   |       |   |    
//      0 |    0---1   @---@---@   @---@---@   @---@---@   @---@---@   @--255
//        |
//         -------------------------------------------------------------------
//             0   1   2   3          ---> Axes[0]                         15
//
// Notes: (1) Unit change in Line yields single unit change in Axes position:
//            the Hilbert curve is maximally local.
//        (2) CPU proportional to total number of bits, = b * n.
//
// History:   John Skilling  20 Apr 2001, 11 Jan 2003, 3 Sep 2003
//-----------------------------------------------------------------------------
// 
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Functions: LinetoTranspose
//            TransposetoLine
//
// Purpose:   Recover Hilbert integer by bit-transposition
//
// Example:   b=5 bits for each of n=3 coordinates
//               15-bit Hilbert integer = A B C D E a b c d e 1 2 3 4 5
//                                        X[0]..... X[1]..... X[2].....
//            transposed to
//               X[0](high) = A D b e 3
//               X[1]       = B E c 1 4
//               X[2](low)  = C a d 2 5
//                            high  low
//
//-----------------------------------------------------------------------------
// 
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Functions: TransposetoAxes
//            AxestoTranspose
//
// Purpose:   Transform between Hilbert transpose and geometrical axes
//
// Example:   b=5 bits for each of n=3 coordinates
//            Hilbert transpose
//             X[0] = A D b e 3                  X[1]|  
//             X[1] = B E c 1 4    <------->         |  /X[2]
//             X[2] = C a d 2 5                axes  | /
//                    high  low                      |/______
//                                                         X[0]
//            Axes are stored conventially as b-bit integers.
//         
//-----------------------------------------------------------------------------





class HilbertCurve
{
	public:
		HilbertCurve(){};
		~HilbertCurve(){};


		void TransposetoLine(
        		int* Line,         //  Hilbert integer 
        		int* X,            //  Transpose      
        		int  b,            //  # bits
        		int  n);            //  dimension
		
		int64_t AxestoLine(
        		int* Axes,    //    multidimensional geometrical axes  
       			int b,        //    # bits used in each word
        		int n);        //    dimension
		
		void LinetoAxes(
        		int* Axes,    //    multidimensional geometrical axes  
		        int* Line,    //    linear serial number, stored as     
        		int b,        //    # bits used in each word
       	 		int n);        //    dimension
		
		
		void TransposetoAxes(
        		int* X,      //   position  
        		int b,       //   # bits
        		int n);       //   dimension
		
		void AxestoTranspose(
        		int* X,   //  position      
        		int b,    //  # bits    
        		int n);    //  dimension 		
		
		void LinetoTranspose(
        		int* X,     //   Transpose        
        		int* Line,  //   Hilbert integer  
        		int b,      //   # bits
        		int n);      //   dimension  	

};
#endif
