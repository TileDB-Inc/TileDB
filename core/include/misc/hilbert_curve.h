/**
 * @file   hilbert_curve.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines the HilbertCurve class. 
 */

#ifndef __HILBERT_CURVE_H__
#define __HILBERT_CURVE_H__

#include <cstdlib>
#include <iostream>
#include <sys/types.h>




/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/** 
 * Maximum number of dimensions for defining the Hilbert curve. Although the
 * Hilbert curve can be defined over arbitrary dimensionality, we limit the
 * number of dimensions because they affect the number of bits used to 
 * represent a Hilbert value; in our class, a Hilbert value is a int64_t
 * number and, thus, it cannot exceed 64 bits.
 */
#define HC_MAX_DIM 16




/**
 * The Hilbert curve fills a multi-dimensional space in a particular manner
 * with a 1D line. The typical operations of this class is converting a 
 * multi-dimensional tuple of coordinates into a 1D Hilbert value, and 
 * vice versa. 
 *
 * For the 2D case, the Hilbert curve looks as follows:
 *
 \verbatim
         |
      15 |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |    |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |
         |    @   @---@   @   @   @---@   @   @   @---@   @   @   @---@   @
         |    |           |   |           |   |           |   |           |
         |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |        |   |           |   |           |   |           |   |    
         |    @---@   @---@---@---@   @---@   @---@   @---@---@---@   @---@
         |    |                           |   |                           |
         |    @   @---@---@   @---@---@   @   @   @---@---@   @---@---@   @
         |    |   |       |   |       |   |   |   |       |   |       |   |
   Dim[1]|    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |            |           |                   |           |        
         |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |    |   |       |   |       |   |   |   |       |   |       |   |
         |    @   @---@---@   @---@---@   @---@   @---@---@   @---@---@   @
         |    |                                                           |
         |    @---@   @---@---@   @---@---@   @---@---@   @---@---@   @---@
         |        |   |       |   |       |   |       |   |       |   |    
         |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |    |           |           |           |           |           |
         |    @   @---@   @   @---@   @---@   @---@   @---@   @   @---@   @
         |    |   |   |   |   |   |       |   |       |   |   |   |   |   |
         |    @---@   @---@   @   @---@---@   @---@---@   @   @---@   @---@
         |                    |                           |                
       3 |    5---6   9---@   @   @---@---@   @---@---@   @   @---@   @---@
         |    |   |   |   |   |   |       |   |       |   |   |   |   |   |
       2 |    4   7---8   @   @---@   @---@   @---@   @---@   @   @---@   @
         |    |           |           |           |           |           |
       1 |    3---2   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |        |   |       |   |       |   |       |   |       |   |    
       0 |    0---1   @---@---@   @---@---@   @---@---@   @---@---@   @--255
         |
          -------------------------------------------------------------------
              0   1   2   3               Dim[0]                          15
 
 \endverbatim
 *
 * The Hilbert value of (2,3) is 9, whereas the coordinates corresponding to 
 * Hilbert value 2 are (1,1).
 *
 * The class utilizes two functions from John Skilling's work published in:
 * John Skilling, "Programming the Hilbert Curve". In AIP, 2004
 */
class HilbertCurve {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** 
   * Constructor.
   *
   * @param bits Number of bits used for coordinate values across each dimension
   * @param dim_num Number of dimensions
   */
  HilbertCurve(int bits, int dim_num);

  /** Destructor. */
  ~HilbertCurve();




  /* ********************************* */
  /*           MAIN FUNCTIONS          */
  /* ********************************* */

  /**  
   * Converts a set of coordinates to a Hilbert value.
   *
   * @param coords The coordinates to be converted.
   * @param hilbert The output Hilbert value.
   * @return void
   */
  void coords_to_hilbert(const int* coords, int64_t& hilbert); 

  /**  
   * Converts a Hilbert value into a set of coordinates.
   *
   * @param hilbert The Hilbert value to be converted.
   * @param coords The output coordinates.
   * @return void
   */
  void hilbert_to_coords(int64_t hilbert, int* coords);




 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Number of bits for representing a coordinate per dimension. */	
  int bits_;
  /** Number of dimensions. */	
  int dim_num_;
  /** Temporary buffer. */
  int temp_[HC_MAX_DIM];




  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Identical to John Skilling's work. It converts the input coordinates
   * to what is called the transpose of the Hilbert value. This is done
   * in place.
   *
   * **Example**
   *
   * Let b=5 and n=3. Let the 15-bit Hilbert value of the input coordinates
   * be A B C D E a b c d e 1 2 3 4 5. The function places this number into
   * parameter X as follows:
   *
   \verbatim   
             X[0] = A D b e 3                  X[1]|  
             X[1] = B E c 1 4    <------->         |  /X[2]
             X[2] = C a d 2 5                axes  | /
                    high  low                      |/______
  
                                                         X[0]
   \endverbatim
   *
   * The X value after the function terminates is called the transpose form
   * of the Hilbert value.
   *
   * @param X Input coordinates, and output transpose (the conversion is
   *     done in place).
   * @param b Number of bits for representing a coordinate per dimension
   *     (it should be equal to HilbertCurve::bits_). 
   * @param n Number of dimensions (it should be equal to 
   *     HilbertCurve::dim_num_). 
   * @return void
   */	
  void AxestoTranspose(int* X, int b, int n);

  /**
   * Identical to John Skilling's work. It converts the transpose of a
   * Hilbert value into the corresponding coordinates. This is done in place.
   *
   * **Example**
   *
   * Let b=5 and n=3. Let the 15-bit Hilbert value of the output coordinates
   * be A B C D E a b c d e 1 2 3 4 5. The function takes as input the tranpose 
   * form of the Hilbert value, which is stored in X as follows:
   *
   \verbatim   
             X[0] = A D b e 3                  X[1]|  
             X[1] = B E c 1 4    <------->         |  /X[2]
             X[2] = C a d 2 5                axes  | /
                    high  low                      |/______
  
                                                         X[0]
   \endverbatim
   *
   * The X value after the function terminates will contain the coordinates
   * corresponding to the Hilbert value. 
   *
   * @param X Input transpose, and output coordinates (the conversion is
   *     done in place).
   * @param b Number of bits for representing a coordinate per dimension
   *     (it should be equal to HilbertCurve::bits_). 
   * @param n Number of dimensions (it should be equal to 
   *     HilbertCurve::dim_num_). 
   * @return void
   */	
  void TransposetoAxes(int* X, int b, int n);
};

#endif
