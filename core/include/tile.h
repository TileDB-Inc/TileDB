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
 */

#ifndef TILE_H
#define TILE_H

#include <inttypes.h>
#include <string>
#include <sstream>
#include <vector>
#include <typeinfo>
#include <csv_file.h>

class CSVLine;

/**
 * Tile is an abstract class. It allows polymorphism for interfacing with
 * objects of CoordinateTile and AttributeTile with various template types.
 * It cannot be used to instantiate objects; it is used to create pointer
 * variables that store CoordinateTile and AttributeTile pointers.
 */
class Tile {
 public:
  class const_iterator;

  // TYPE DEFINITIONS
  /** A tile can be either an attribute or a coordinate tile. */
  enum TileType {ATTRIBUTE, COORDINATE};
  /** 
   * A mutlidimensional range (i.e., a hyper-rectangle in the logical space). 
   * It is a list of low/high values across each dimension, i.e.,
   * (dim#1_low, dim#1_high, dim#2_low, dim#2_high, ...).
   */
  typedef std::vector<double> Range;

  // CONSTRUCTORS AND DESTRUCTORS
  /** Simple constructor that takes as input the tile and cell types. */
  Tile(TileType tile_type, const std::type_info* cell_type, 
       uint64_t tile_id, uint64_t cell_num, uint64_t cell_size) 
      : tile_type_(tile_type), cell_type_(cell_type), 
        tile_id_(tile_id), cell_num_(cell_num), cell_size_(cell_size) {}
  /** Empty virtual destructor. */
  virtual ~Tile() {}

  // ACCESSORS
  /** 
   * Returns the bounding coordinates, i.e., the first and 
   * last coordinates that were appended to the tile. It applies only to
   * CoordinateTile objects. The bounding coordinates are typically useful
   * when the cells in the tile are sorted in a certain order. 
   */
  virtual std::pair<std::vector<double>, 
                    std::vector<double> > bounding_coordinates() const =0;
  /** Returns the number of cells in the tile. */
  uint64_t cell_num() const { return cell_num_; }
  /** Returns the cell size (in bytes). */
  uint64_t cell_size() const { return cell_size_; } 
  /** Returns the cell type. */
  const std::type_info* cell_type() const { return cell_type_; } 
  /** Copies the tile payload (i.e., all the cell values) into buffer. */
  virtual void copy_payload(char* buffer) const =0;
  /** 
   * Returns the number of dimensions. It applies only to CoordinateTile 
   * objects.
   */
  virtual unsigned int dim_num() const =0;
  /** 
   * Returns the MBR (minimum bounding rectangle) of the coordinates in the
   * logical multi-dimensional space. It applies only to CoordinateTile objects. 
   * The MBR is a list of low/high values in each dimension, i.e.,
   * (dim#1_low, dim#1_high, dim#2_low, dim#2_high, ...).
   */
  virtual const std::vector<double>& mbr() const =0;
  /** Returns the tile id. */
  uint64_t tile_id() const { return tile_id_; }
  /** Returns the tile size (in bytes). */
  uint64_t tile_size() const { return cell_num_ * cell_size_; } 
  /** Returns the tile type (see Tile::TileType). */
  TileType tile_type() const { return tile_type_; } 
 
  // MUTATORS
  /** MBR setter. Applicable only to CoordinateTile objects. */
  virtual void set_mbr(const std::vector<double>& mbr) =0; 
  /** Payload setter. */
  virtual void set_payload(const char* payload, uint64_t payload_size) =0; 

  // OPERATORS
  /** 
   * Appends an attribute value or coordinates to (the end of) a tile. 
   * This is supposed to be a pure virtual template function, 
   * implemented by AttributeTile and CoordinateTile. Unfortunately, 
   * C++ does not allow virtual template functions. Therefore, we make a trick 
   * to simulate this: we convert template-based dispatch of Tile::operator<< to 
   * virtual function Tile::append_cell dispatch.
   */
  template<typename T>
  void operator<<(const T& value); 
  /** 
   * Appends an attribute value or coordinates to (the end of) a tile, retrieved
   * from the input CSV line. It returns false if it fails to retrieve from
   * the CSV line.
   */
  bool operator<<(CSVLine& csv_line); 
  /** 
   * Appends an attribute value or coordinates to (the end of) a tile, retrieved
   * from the input cell iterator.
   */
  void operator<<(const Tile::const_iterator& cell_it); 

  // CELL ITERATORS
  /** This class implements a constant cell iterator. */
  class const_iterator {
   public:
    // TYPE DEFINITIONS
    /** 
     * This class essentially implements a trick for 
     * Tile::const_iterator::operator*(). If we made that operator templated,
     * we would not be able to resolve the return type upon its call. 
     * Therefore, we create this class and implement a conversion operator. 
     * When an object of this class is assigned to a variable, its conversion 
     * operator is triggered while properly resolving the return type. Since 
     * the return type is resolved, we can do whatever we would do if we had 
     * templated Tile::const_iterator::operator*().
     */
    class const_iterator_ret {
     public:
      /** 
       * Constructor that takes as input the tile for which the 
       * Tile::const_iterator::const_iterator_ret object was created, and the 
       * the cell position in the tile. 
       */
      const_iterator_ret(const Tile* tile, uint64_t pos) 
          : tile_(tile), pos_(pos) {}
      /** 
       * Conversion operator. Called when an object of this class
       * is assigned to a variable.
       */
      template<class T>
      operator T();
     private:
      /** 
       * The current cell position carried through the Tile::const_iterator 
       * object that creates the Tile::const_iterator::const_iterator_ret 
       * object.
       */
      uint64_t pos_;
      /** 
       * The tile pointer carried through the Tile::const_iterator object
       * that creates the Tile::const_iterator::const_iterator_ret object.
       */
      const Tile* tile_;
    };

    // CONSTRUCTORS & DESTRUCTORS
    /** Empty iterator constuctor. */
    const_iterator();
    /** 
     * Iterator constuctor that takes as input the tile for which the 
     * Tile::const_iterator object was created, and the current cell position
     * in the tile. 
     */
    const_iterator(const Tile* tile, uint64_t pos);
    
    // ACCESSORS
    /** Returns the current position of the cell iterator. */
    uint64_t pos() const { return pos_; }
    /** Returns the tile the cell iterator belongs to. */
    const Tile* tile() const { return tile_; }

    // OPERATORS
    /** Assignment operator. */
    void operator=(const const_iterator& rhs);
    /** Addition operator. */
    const_iterator operator+(int64_t step);
    /** Addition-assignment operator. */
    void operator+=(int64_t step);
    /** Pre-increment operator. */
    const_iterator operator++();
    /** Post-increment operator. */
    const_iterator operator++(int junk);
    /**
     * We distinguish two cases: (i) If the both operands belong to the same
     * tile, it returns true if their pos_ values are the same. (ii) Otherwise,
     * it returns true if their pointed cell values are equal.
     */
    bool operator==(const const_iterator& rhs) const;
    /**
     * We distinguish two cases: (i) If the both operands belong to the same
     * tile, it returns true if their pos_ values are not the same. 
     * (ii) Otherwise, it returns true if their pointed cell values are not 
     * equal.
     */
    bool operator!=(const const_iterator& rhs) const;
    /** 
     * Returns a Tile::const_iterator::const_iterator_ret object. 
     * Practically, it returns the cell value currently pointed by the iterator.
     * The only limitation is that the result of this operator should be
     * assigned to a variable, in order for the return type to be properly
     * resolved (e.g., double v = *cell_it, for cell iterator cell_it).
     */
    const_iterator_ret operator*() const 
        { return const_iterator_ret(tile_, pos_); } 
    /** Appends the current attribute value or cell to the input CSV line. */
    void operator>>(CSVLine& csv_line) const;

    // MISC
    /** 
     * Returns true if the current coordinates pointed by the iterator fall 
     * inside the input range. It applies only to CoordinateTile objects.
     */
    bool cell_inside_range(const Tile::Range& range) const;

   private:
    /** The current position of the iterator in the payload vector. */
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

  // MISC
  /** 
   * Appends the pos-th attribute value or coordinates to the input CSV line. 
   */
  virtual void append_cell_to_csv_line(uint64_t pos, CSVLine& csv_line) const =0;
  /** 
   * Returns true if the pos-th coordinates fall inside the input range. 
   * It applies only to CoordinateTile objects.
   */
  virtual bool cell_inside_range(uint64_t pos, const Range& range) const =0;
  /** Prints the details of the tile on the standard output. */
  virtual void print() const =0;
 
 protected:
  // PROTECTED ATTRIBUTES
  /** The number of cells in the tile. */
  uint64_t cell_num_;
  /** The cell size (in bytes). */
  uint64_t cell_size_;
  /** The cell type. */
  const std::type_info* cell_type_;
  /** The tile id. */
  uint64_t tile_id_;
  /** The tile type (see Tile::TileType). */
  TileType tile_type_;

  // PROTECTED METHODS
  /** 
   * Appends an attribute value retrieved from a CSV line. It returns 1 if the
   * valuesis successfully retrieved.
   */
  template<class T>
  bool append_attribute_value_from_csv_line(CSVLine& csv_line);
  /** Appends an attribute value retrieved from a cell iterator. */
  template<class T>
  void append_attribute_value_from_cell_it(const Tile::const_iterator& cell_it);

  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual void append_cell(const char& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual void append_cell(const int& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual void append_cell(const int64_t& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual void append_cell(const float& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual void append_cell(const double& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   * Inapplicable to AttributeTile objects.
   */
  virtual void append_cell(const std::vector<int>& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   * Inapplicable to AttributeTile objects.
   */
  virtual void append_cell(const std::vector<int64_t>& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   * Inapplicable to AttributeTile objects.
   */
  virtual void append_cell(const std::vector<float>& value) =0;
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   * Inapplicable to AttributeTile objects.
   */
  virtual void append_cell(const std::vector<double>& value) =0;
  /** 
   * Appends coordinates retrieved from a CSV line. It returns 
   * CoordinateTile::dim_num_ if the coordinates are successfully retrieved.
   */
  template<class T>
  bool append_coordinates_from_csv_line(CSVLine& csv_line);
  /** Appends coordinates retrieved from a cell iterator. */
  template<class T>
  void append_coordinates_from_cell_it(const Tile::const_iterator& cell_it);
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual char cell_char(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual int cell_int(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual int64_t cell_int64_t(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual float cell_float(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   * Inapplicable to CoordinateTile objects.
   */
  virtual double cell_double(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   * Inapplicable to AttributeTile objects.
   */
  virtual std::vector<int> cell_v_int(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   * Inapplicable to AttributeTile objects.
   */
  virtual std::vector<int64_t> cell_v_int64_t(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   * Inapplicable to AttributeTile objects.
   */
  virtual std::vector<float> cell_v_float(uint64_t pos_) const =0;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   * Inapplicable to AttributeTile objects.
   */
  virtual std::vector<double> cell_v_double(uint64_t pos_) const =0;
};

/**
 * This class defines an attribute tile.
 * Attribute tiles consist of a collection of cells of template type T, 
 * which are stored in vector AttributeTile::payload_.
 */
template<class T>
class AttributeTile : public Tile {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Simple constructor that takes the tile id as an input. */
  explicit AttributeTile(uint64_t tile_id);
  /** 
   * Constructor that takes as input the tile id and tile capacity. 
   * The tile capacity is the number of cells expected to reside
   * in the tile. It does not put any constraints on the number of cells
   * allowed in the tile. It is only used to reserve space upon initialization
   * for AttributeTile::payload_. It prevents unnecessary expansions of
   * AttributeTile::payload_ when new cells are appended to it.
   */
  AttributeTile(uint64_t tile_id, uint64_t capacity);
  /** Empty destructor. */	
  ~AttributeTile() {}

  // ACCESSORS
  /** Returns the i-th cell. */
  const T& cell(uint64_t i) const;
  /** Copies the tile payload into buffer. */
  virtual void copy_payload(char* buffer) const;

  // MUTATORS
  /** Inapplicable to attribute tiles. Attribute tiles do not have MBRs. */
  virtual void set_mbr(const std::vector<double>& mbr); 
  /** Paylod setter. */
  virtual void set_payload(const char* payload, uint64_t payload_size); 

  // OPERATORS
  /** Appends a cell value to (end of the) tile. */
  template<typename T2>
  void operator<<(const T2& value); 
   
  // MISC
  /** Appends the pos-th attribute value to the input CSV line. */
  virtual void append_cell_to_csv_line(uint64_t pos, CSVLine& csv_line) const;
  /** Inapplicable to attribute tiles. */
  virtual bool cell_inside_range(uint64_t pos, const Range& range) const;
  /** Prints the details of the tile on the standard output. */
  virtual void print() const;

 private:
  // PRIVATE ATTRIBUTES
  /** The payload is essentially the list of the cell values of the tile. */
  std::vector<T> payload_;

  // PRIVATE METHODS
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  virtual void append_cell(const char& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  virtual void append_cell(const int& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  virtual void append_cell(const int64_t& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  virtual void append_cell(const float& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * AttributeTile::operator<< when Tile* points to an AttributeTile.
   */
  virtual void append_cell(const double& value);
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have vector cells.
   */
  virtual void append_cell(const std::vector<int>& coordinates);
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have vector cells.
   */
  virtual void append_cell(const std::vector<int64_t>& coordinates);
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have vector cells.
   */
  virtual void append_cell(const std::vector<float>& coordinates);
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have vector cells.
   */
  virtual void append_cell(const std::vector<double>& coordinates);
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have bounding 
   * coordinates. 
   */
  virtual std::pair<std::vector<double>, 
                    std::vector<double> > bounding_coordinates() const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual char cell_char(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual int cell_int(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual int64_t cell_int64_t(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual float cell_float(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call AttributeTile::cell when Tile* points to an 
   * AttributeTile.
   */
  virtual double cell_double(uint64_t pos_) const;
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have vector cells.
   */
  virtual std::vector<int> cell_v_int(uint64_t pos_) const;
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have vector cells.
   */
  virtual std::vector<int64_t> cell_v_int64_t(uint64_t pos_) const;
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have vector cells.
   */
  virtual std::vector<float> cell_v_float(uint64_t pos_) const;
  /** 
   * Inapplicable to attribute tiles. Attribute tiles do not have vector cells.
   */
  virtual std::vector<double> cell_v_double(uint64_t pos_) const;
  /** Inapplicable to attribute tiles. Attribute tiles do not have dimensions. */
  virtual unsigned int dim_num() const; 
  /** Inapplicable to attribute tiles. Attribute tiles do not have MBRs. */
  virtual std::vector<double>& mbr() const;
};

/**
 * This class defines a coordinate tile.
 * In a coordinate tile, each cell is a set of coordinates of type T, whose 
 * number is specified by CoordinateTile::dim_num_ (> 0). A set of coordinates 
 * is modeled as a vector of type T. All coordinates are stored in a vector of 
 * vectors, namely CoordinateTile::payload_.
 *
 * Every coordinate tile has a MBR (minimum bounding rectangle). This is
 * essentially the tightest hyper-rectangle (represented by pairs of low/high
 * bounds for each dimension) in the logical space that contains all the 
 * coordinates in the tile.
 */
template<class T>
class CoordinateTile : public Tile {
 public:
  /** 
   * Simple constructor that takes as input a tile id and the number of
   * dimensions. 
   */
  CoordinateTile(uint64_t tile_id, unsigned int dim_num);
  /** 
   * Constructor that takes as input the tile id, dimension number and tile 
   * capacity. The tile capacity is the number of cells expected 
   * to reside in the tile. It does not put any constraints on the number of
   * cells allowed in the tile. It is only used to reserve space upon 
   * initialization for CoordinateTile::payload_. It prevents unnecessary 
   * expansions of CoordinateTile::payload_ when new cells are appended to it.
   */
  CoordinateTile(uint64_t tile_id, unsigned int dim_num, uint64_t capacity);
  /** Empty destructor. */
  ~CoordinateTile() {}

  // ACCESSORS
  /** 
   * Returns the bounding coordinates, i.e., the first and last coordinates  
   * that were appended to the tile. The bounding coordinates are typically 
   * useful when the cells in the tile are sorted in a certain order. 
   */
  virtual std::pair<std::vector<double>, 
                    std::vector<double> > bounding_coordinates() const;
  /** Returns the i-th cell. */
  const std::vector<T>& cell(uint64_t i) const;
  /** Copies the tile payload into buffer. */
  virtual void copy_payload(char* buffer) const;
  /** Returns the number of dimensions. */
  virtual unsigned int dim_num() const { return dim_num_; } 
  /** 
   * Returns the MBR (minimum bounding rectangle), i.e., the tightest 
   * hyper-rectangle in the logical space that contains all the 
   * coordinates in the tile. The MBR is represented as a vector
   * of low/high pairs of values in each dimension, i.e., 
   * (dim#1_low, dim#1_high, dim#2_low, dim#2_high, ...).
   */
  virtual const std::vector<double>& mbr() const { return mbr_; }

  // MUTATORS
  /** MBR setter. */
  virtual void set_mbr(const std::vector<double>& mbr); 
  /** Payload setter. */
  virtual void set_payload(const char* payload, uint64_t payload_size); 
 
  // OPERATORS
   /** Appends a set of coordinates to the (end of the) tile. */
  template<typename T2>
  void operator<<(const std::vector<T2>& coordinates);
  
  // MISC
  /** Appends the pos-th coordinates to the input CSV line. */
  virtual void append_cell_to_csv_line(uint64_t pos, CSVLine& csv_line) const;
  /** Returns true if the pos-th coordinates fall inside the input range. */
  virtual bool cell_inside_range(uint64_t pos, const Range& range) const;
  /** Prints the details of the tile on the standard output. */
  virtual void print() const;

 private:
  // PRIVATE ATTRIBUTES
  /** The number of dimensions. */
  unsigned int dim_num_;	
  /** 
   * The tile MBR (minimum bounding rectangle), i.e., the tightest 
   * hyper-rectangle in the logical space that contains all the 
   * coordinates in the tile. The MBR is represented as a vector
   * of low/high pairs of values in each dimension, i.e., 
   * (dim#1_low, dim#1_high, dim#2_low, dim#2_high, ...).
    */
  std::vector<double> mbr_;
  /** 
   * The payload is a vector of coordinates (which are implemented also
   * as vectors). 
   */
  std::vector<std::vector<T> > payload_;

  // PRIVATE METHODS  
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual void append_cell(const char& value); 
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual void append_cell(const int& value); 
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual void append_cell(const int64_t& value);
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual void append_cell(const float& value);
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual void append_cell(const double& value);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  virtual void append_cell(const std::vector<int>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  virtual void append_cell(const std::vector<int64_t>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  virtual void append_cell(const std::vector<float>& coordinates);
  /** 
   * Template-based dispatch of Tile::operator<< to virtual function dispatch.
   * This allows Tile::operator<< to essentially call
   * CoordinateTile::operator<< when Tile* points to a CoordinateTile.
   */
  virtual void append_cell(const std::vector<double>& coordinates);
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual char cell_char(uint64_t pos_) const;
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual int cell_int(uint64_t pos_) const;
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual int64_t cell_int64_t(uint64_t pos_) const;
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual float cell_float(uint64_t pos_) const;
  /** 
   * Inapplicable to coordinate tiles. Coordinate tiles have vector cells.
   */
  virtual double cell_double(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual std::vector<int> cell_v_int(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual std::vector<int64_t> cell_v_int64_t(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual std::vector<float> cell_v_float(uint64_t pos_) const;
  /** 
   * Template-based dispatch of Tile::const_iterator::operator*() to virtual 
   * function dispatch. This allows Tile::const_iterator::operator*() 
   * to essentially call CoordinateTile::cell when Tile* points to an 
   * CoordinateTile.
   */
  virtual std::vector<double> cell_v_double(uint64_t pos_) const;
  /** Updates the tile MBR bounds. */
  void update_mbr(const std::vector<T>& coordinates);  
};

#endif
