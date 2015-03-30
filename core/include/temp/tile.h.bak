/**
 * @file   tile.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file defines classes Tile, AttributeTile and CoordinateTile.
 * It also defines TileException, which is thrown by AttributeTile
 * and CoordinateTile.
 */

#ifndef TILE_H
#define TILE_H

#include <inttypes.h>
#include <string>
#include <sstream>
#include <vector>

/**
 * Tile is an abstract class. It allows polymorphism to easily access
 * objects of CoordinateTile and AttributeTile with various template types.
 */
class Tile {
 public:
  // CONSTRUCTORS AND DESTRUCTORS
  Tile() {}
  /** Simple constructor that takes the tile id as an input. */
  explicit Tile(uint64_t tile_id) 
      : tile_id_(tile_id), cell_num_(0), cell_size_(0) {}	
  virtual ~Tile() {}

  // ACCESSORS
  /** 
   * Returns the bounding coordinates, i.e., the first and 
   * last coordinates that were appended to the tile. It applies only to
   * CoordinateTile objects. 
   */
  virtual std::pair<std::vector<double>, 
                    std::vector<double> > bounding_coordinates() const =0;
  /** Returns the number of cells. */
  uint64_t cell_num() const { return cell_num_; }
  /** Returns the cell size (in bytes). */
  uint64_t cell_size() const { return cell_size_; } 
  /** Copies the tile payload into buffer. */
  virtual void copy_payload(char* buffer) const =0;
  /** Returns the number of dimensions. */
  virtual unsigned int dim_num() const =0;
  /** 
   * Returns the MBR (minimum bounding rectangle) of the coordinates in the
   * logical multi-dimensional space. It applies only to CoordinateTile objects. 
   */
  virtual const std::vector<double>& mbr() const =0;
  /** Prints the details of the tile on the standard output. */
  virtual void print() const =0;
  /** Returns the tile id. */
  uint64_t tile_id() const { return tile_id_; }
  /** Returns the tile size (in bytes). */
  uint64_t tile_size() const { return cell_num_ * cell_size_; } 
 
  // OPERATORS
  /** 
   * Appends a cell value or coordinates to (the end of) a tile. 
   * This is supposed to be a pure virtual template function, 
   * implemented by AttributeTile and CoordinateTile. Unfortunately, 
   * C++ does not allow virtual template functions. Therefore, we make a trick 
   * to do so: we convert template-based dispatch of Tile::operator<< to 
   * virtual function Tile::append_cell dispatch.
  */
  template<typename T2>
  void operator<<(const T2& value); 

  // CELL ITERATORS
  /** This class implements a constant cell iterator. */
  class const_iterator {
   public:
    /** 
     * This class essentially implements a trick for 
     * Tile::const_iterator::operator*. If we made that operator templated,
     * we would not be able to resolve the return type upon its call. 
     * Therefore, we create this class and implement a conversion operator. 
     * When an object of this class is assigned to a variable, its conversion 
     * operator is triggered while properly resolving the return type. Since 
     * the return type is resolved, we can do whatever we would do if we had 
     * templated Tile::const_iterator::operator*.
     */
    class const_iterator_ret {
     public:
      /** Constructor. */
      const_iterator_ret(const Tile* tile, uint64_t pos) 
          : tile_(tile), pos_(pos) {}
      /** 
       * Conversion operator. Called when an object of this class
       * is assigned to a variable.
       */
      template<class T3>
      operator T3();
     private:
      /** 
       * The tile pointer carried through the Tile::const_iterator object
       * that creates the Tile::const_iterator::const_iterator_ret object.
       */
      const Tile* tile_;
      /** 
       * The current cell position carried through the Tile::const_iterator 
       * object that creates the Tile::const_iterator::const_iterator_ret 
       * object.
       */
      uint64_t pos_;
    };
    /** Iterator constuctor. */
    const_iterator();
    /** Iterator constuctor. */
    const_iterator(const Tile* tile, uint64_t pos);
    /** Assignment operator. */
    void operator=(const const_iterator& rhs);
    /** Pre-increment operator. */
    const_iterator operator++();
    /** Post-increment operator. */
    const_iterator operator++(int junk);
    /** 
     * Returns true if the iterator is equal to that in the
     * right hand side (rhs) of the operator. 
     */
    bool operator==(const const_iterator& rhs) const;
    /** 
     * Returns true if the iterator is equal to that in the
     * right hand side (rhs) of the operator. 
     */
    bool operator!=(const const_iterator& rhs) const;
    /** 
     * Returns a Tile::const_iterator::const_iterator_ret object. 
     * Practically, it returns the cell value currently pointed by the iterator.
     */
    const_iterator_ret operator*() const 
        { return const_iterator_ret(tile_, pos_); } 

   private:
    /** The current position of the iterator on the payload vector. */
    uint64_t pos_;  
    /** The tile object the iterator is created for. */
    const Tile* tile_;
  };
  /** Returns a cell iterator pointing to the first cell of the tile. */
  const_iterator begin() const;
  /**
   * Returns a cell iterator pointing to one position after the last 
   * cell of the tile. 
   */
  const_iterator end() const;
 
 protected:
  // PROTECTED ATTRIBUTES
  /** The number of cells in the tile. */
  uint64_t cell_num_;
  /** The cell size (in bytes). */
  uint64_t cell_size_;
  /** The tile id. */
  uint64_t tile_id_;

  // PROTECTED METHODS
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< or CoordinateTile::operator<< when Tile*
   * points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual void append_cell(const int& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< or CoordinateTile::operator<< when Tile*
   * points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual void append_cell(const int64_t& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< or CoordinateTile::operator<< when Tile*
   * points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual void append_cell(const float& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< or CoordinateTile::operator<< when Tile*
   * points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual void append_cell(const double& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< or CoordinateTile::operator<< when Tile*
   * points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual void append_cell(const std::vector<int>& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< or CoordinateTile::operator<< when Tile*
   * points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual void append_cell(const std::vector<int64_t>& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< or CoordinateTile::operator<< when Tile*
   * points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual void append_cell(const std::vector<float>& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< or CoordinateTile::operator<< when Tile*
   * points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual void append_cell(const std::vector<double>& value) =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell or CoordinateTile::cell 
   * when Tile* points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual const int& get_cell_int(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell or CoordinateTile::cell 
   * when Tile* points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual const int64_t& get_cell_int64_t(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell or CoordinateTile::cell 
   * when Tile* points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual const float& get_cell_float(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell or CoordinateTile::cell 
   * when Tile* points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual const double& get_cell_double(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell or CoordinateTile::cell 
   * when Tile* points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual const std::vector<int>& get_cell_v_int(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell or CoordinateTile::cell 
   * when Tile* points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual const std::vector<int64_t>& get_cell_v_int64_t(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell or CoordinateTile::cell 
   * when Tile* points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual const std::vector<float>& get_cell_v_float(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell or CoordinateTile::cell 
   * when Tile* points to an AttributeTile or a CoordinateTile, respectively.
   */
  virtual const std::vector<double>& get_cell_v_double(uint64_t pos_) const =0;
};

/**
 * This class defines an attribute tile.
 * Attribute tiles consist of a collection of cells of template type T, 
 * which are stored in vector AttributeTile::payload_.
 * It throws a TileException.
 */
template<class T>
class AttributeTile : public Tile {
 public:
  // CONSTRUCTORS
  /** Simple constructor that takes the tile id as an input. */
  explicit AttributeTile(uint64_t tile_id);	
  ~AttributeTile() {}

  // ACCESSORS
  /** 
   * Throws a TileException. Attribute tiles do not have bounding 
   * coordinates. 
   */
  std::pair<std::vector<double>, 
            std::vector<double> > bounding_coordinates() const;
  /** Returns the i-th cell. */
  const T& cell(uint64_t i) const;
  /** Copies the tile payload into buffer. */
  void copy_payload(char* buffer) const;
  /** Throws a TileException. Attribute tiles do not have dimensions. */
  unsigned int dim_num() const; 
  /** Throws a TileException. Attribute tiles do not have MBRs. */
  std::vector<double>& mbr() const;
  /** Returns the payload. */
  const std::vector<T>& payload() const { return payload_; }

   // MUTATORS
  /** Simple payload setter. */
  void set_payload(const std::vector<T>& payload);

  // OPERATORS
  /** Appends a cell value to (end of the) tile. */
  template<typename T2>
  void operator<<(const T2& value); 
   
  // ITERATORS
  /** Returns payload begin iterator. */
  typename std::vector<T>::iterator begin() { return payload_.begin(); }
  /** Returns payload begin constant iterator. */
  typename std::vector<T>::const_iterator begin() const 
      { return payload_.begin(); }
  /** Returns payload end iterator. */
  typename std::vector<T>::iterator end() { return payload_.end(); }
  /** Returns payload end constant iterator. */
  typename std::vector<T>::const_iterator end() const 
      { return payload_.end(); }

  // MISC
  /** Prints the details of the tile on the standard output. */
  void print() const;

 private:
  // PRIVATE ATTRIBUTES
  /** The payload is essentially the list of cell values of the tile. */
  std::vector<T> payload_;

  // PRIVATE METHODS
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  void append_cell(const int& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  void append_cell(const int64_t& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  void append_cell(const float& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  void append_cell(const double& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  void append_cell(const std::vector<int>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  void append_cell(const std::vector<int64_t>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  void append_cell(const std::vector<float>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  void append_cell(const std::vector<double>& coordinates);
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual const int& get_cell_int(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual const int64_t& get_cell_int64_t(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual const float& get_cell_float(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual const double& get_cell_double(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual const std::vector<int>& get_cell_v_int(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual const std::vector<int64_t>& get_cell_v_int64_t(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual const std::vector<float>& get_cell_v_float(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual const std::vector<double>& get_cell_v_double(uint64_t pos_) const;
};

/**
 * This class defines a coordinate tile.
 * In a coordinate tile, each cell is a set of coordinates of type T, whose 
 * number is specified by CoordinateTile::dim_num_ (!= 0). A set of coordinates 
 * is modeled as a vector of type T. All coordinates are stored in a vector of 
 * vectors, namely CoordinateTile::payload_.
 *
 * Every coordinate tile has a MBR (minimum bounding rectangle). This is
 * essentially the tightest hyper-rectangle (represented by pairs of high/low
 * bounds for each dimension) in the logical space that contains all the 
 * coordinates in the tile.
 *
 * It throws a TileException.
 */
template<class T>
class CoordinateTile : public Tile {
 public:
  /** 
   * Simple constructor that takes as input a tile id and the number of
   * dimensions. 
   */
  CoordinateTile(uint64_t tile_id, unsigned int dim_num);
  ~CoordinateTile() {}

  // ACCESSORS
  /** 
   * Returns the bounding coordinats, i.e., the first and last coordinates 
   * in the tile. 
   */
  std::pair<std::vector<double>, 
            std::vector<double> > bounding_coordinates() const;
  /** Returns the i-th cell. */
  const std::vector<T>& cell(uint64_t i) const;
  /** Copies the tile payload into buffer. */
  void copy_payload(char* buffer) const;
  /** Returns the number of dimensions. */
  unsigned int dim_num() const { return dim_num_; } 
  /** 
   * Returns the MBR (minimum bounding rectangle, i.e., the tightest 
   * hyper-rectangle (represented by pairs of high/low
   * bounds for each dimension) in the logical space that contains all the 
   * coordinates in the tile.
   */
  const std::vector<double>& mbr() const { return mbr_; }
  /** Returns the payload. */
  const std::vector<std::vector<T> >& payload() const { return payload_; }

  // MUTATORS
  /** Simple MBR setter. */
  void set_mbr(const std::vector<double>& mbr) { mbr_ = mbr; }
  /** Simple payload setter. */
  void set_payload(const std::vector<std::vector<T> >& payload);
   
  // OPERATORS
   /** Appends a set of coordinates to (end of the) tile. */
  template<typename T2>
  void operator<<(const std::vector<T2>& coordinates);
  
  // ITERATORS
  /** Returns payload begin iterator. */
  typename std::vector<std::vector<T> >::iterator begin() 
      { return payload_.begin(); }
  /** Returns payload begin constant iterator. */
  typename std::vector<std::vector<T> >::const_iterator begin() const 
      { return payload_.begin(); }
  /** Returns payload end iterator. */
  typename std::vector<std::vector<T> >::iterator end() 
      { return payload_.end(); }
  /** Returns payload end constant iterator. */
  typename std::vector<std::vector<T> >::const_iterator end() const 
      { return payload_.end(); }

  // MISC
  /** Prints the details of the tile on the standard output. */
  void print() const;

 private:
  // PRIVATE ATTRIBUTES
  /** The number of dimensions. */
  unsigned int dim_num_;	
  /** 
    * The tile MBR (minimum bounding rectangle). This is essentially the 
    * tightest hyper-rectangle (represented by pairs of high/low bounds 
    * for each dimension) in the logical space that contains all the 
    * coordinates in the tile. 
    */
  std::vector<double> mbr_;
  /** 
   * The payload is a vector of coordinates (which are implemented also
   * as vectors. 
   */
  std::vector<std::vector<T> > payload_;

  // PRIVATE METHODS  
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  void append_cell(const int& value); 
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  void append_cell(const int64_t& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  void append_cell(const float& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  void append_cell(const double& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  void append_cell(const std::vector<int>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  void append_cell(const std::vector<int64_t>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  void append_cell(const std::vector<float>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  void append_cell(const std::vector<double>& coordinates);
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual const int& get_cell_int(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual const int64_t& get_cell_int64_t(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual const float& get_cell_float(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual const double& get_cell_double(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual const std::vector<int>& get_cell_v_int(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual const std::vector<int64_t>& get_cell_v_int64_t(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual const std::vector<float>& get_cell_v_float(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator* to virtual 
   * function dispatch. This allows Tile::const_iterator::operator* 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual const std::vector<double>& get_cell_v_double(uint64_t pos_) const;

  /** Updates the tile MBR bounds. */
  void update_mbr(const std::vector<T>& coordinates);  
};

/** This exception is thrown by CoordinateTile and AttributeTile. */
class TileException {
 public:
  // CONSTRUCTORS
  /** 
   * Simple constructor that takes the exception message and tile id 
   * as inputs. 
   */
  TileException(const std::string& msg, uint64_t tile_id) 
      : msg_(msg), 
        tile_id_(tile_id) {}
  ~TileException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }
  /** Returns information about where the exception occured. */
  std::string where() const {
    std::stringstream where_msg;
    where_msg << tile_id_;
    return where_msg.str(); 
  }

 private:
  /** Exception message. */
  std::string msg_;
  /** Tile id for which the exception occured. */
  uint64_t tile_id_;
};

#endif
