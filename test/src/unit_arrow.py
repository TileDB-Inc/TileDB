import numpy as np
import pyarrow as pa
import numpy as np
import sys, os
import tempfile
import random

from numpy.testing import assert_array_equal

# ************************************************************************** #
#          Data generators                                                   #
# ************************************************************************** #

# python 2 vs 3 compatibility
if sys.hexversion >= 0x3000000:
    getchr = chr
else:
    getchr = unichr

def gen_chr(max, printable=False):
    while True:
        # TODO we exclude 0x0 here because the key API does not embedded NULL
        s = getchr(random.randrange(1, max))
        if printable and not s.isprintable():
            continue
        if len(s) > 0:
            break
    return s

def rand_datetime64_array(size, start=None, stop=None, dtype=None):
    if not dtype:
        dtype = np.dtype('M8[ns]')

    # generate randint inbounds on the range of the dtype
    units = np.datetime_data(dtype)[0]
    intmin, intmax = np.iinfo(np.int64).min, np.iinfo(np.int64).max

    if start is None:
        start = np.datetime64(intmin + 1, units)
    else:
        start = np.datetime64(start)
    if stop is None:
        stop = np.datetime64(intmax, units)
    else:
        stop = np.datetime64(stop)

    arr = np.random.randint(
        start.astype(dtype).astype(np.int64), stop.astype(dtype).astype(np.int64),
        size=size, dtype=np.int64
    )
    arr.sort()

    return arr.astype(dtype)

def rand_utf8(size=5):
    return u''.join([gen_chr(0xD7FF) for _ in range(0, size)])

def rand_ascii_bytes(size=5, printable=False):
    return b''.join([gen_chr(127, printable).encode('utf-8') for _ in range(0,size)])

# ************************************************************************** #
#           Test class                                                       #
# ************************************************************************** #

class DataFactory():
  def __init__(self, col_size):
    self.results = {}
    self.col_size = col_size
    self.create()

  def __len__(self):
    if not self.data:
      raise ValueError("Uninitialized data")
    return len(self.data)

  def create(self):
    # generate test data for all columns
    col_size = self.col_size
    self.data = {}

    for dt in (np.int8, np.uint8, np.int16, np.uint16, np.int32, np.uint32, np.int64, np.uint64):
        key = np.dtype(dt).name
        dtinfo = np.iinfo(dt)
        self.data[key] = np.random.randint(dtinfo.min, dtinfo.max, size=col_size, dtype=dt)

    for dt in (np.float32, np.float64):
        key = np.dtype(dt).name
        self.data[key] = np.random.rand(col_size).astype(dt)

    # var-len (strings)
    self.data['tiledb_char'] = np.array([rand_ascii_bytes(np.random.randint(1,100))
                                       for _ in range(col_size)]).astype("S1")
    self.data['utf_string1'] = np.array([rand_utf8(np.random.randint(1, 100))
                                       for _ in range(col_size)]).astype("U0")

    # another version with some important cells set to empty
    self.data['utf_string2'] = np.array([rand_utf8(np.random.randint(0, 100))
                                       for _ in range(col_size)]).astype("U0")

    utf_string2 = self.data['utf_string2']
    for i in range(len(utf_string2)):
        self.data['utf_string2'][i] = ''
    range_start = len(utf_string2) - 1
    range_end = len(utf_string2) % 3
    for i in range(range_start, range_end, -1):
        self.data['utf_string2'][i] = ''

    self.data['datetime_ns'] = rand_datetime64_array(col_size)

    ##########################################################################

    self.arrays = [pa.array(val) for val in self.data.values()]
    self.names = list(self.data.keys())

  def import_result(self, name, c_array, c_schema):
    self.results[name] = pa.Array._import_from_c(c_array, c_schema)

  def check(self):
    for key,val in self.data.items():
      assert (key in self.results), "Expected key '{}' not found in results!".format(key)

      res_val = self.results[key]
      assert_array_equal(val, res_val)

    return True