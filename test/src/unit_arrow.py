import numpy as np
import pyarrow as pa
import numpy as np
import sys, os
import tempfile
import random

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

def rand_utf8(size=5):
    return u''.join([gen_chr(0xD7FF) for _ in range(0, size)])

class DataFactory():
  def __init__(self, col_size):
    self.col_size = col_size
    self.create()

  def __len__(self):
    if not self.data:
      raise ValueError("Uninitialized data")
    return len(self.data)

  def create(self):
    col_size = self.col_size
    self.data = {}
    for dt in (np.int8, np.uint8, np.int16, np.uint16, np.int32, np.uint32, np.int64, np.uint64):
        key = np.dtype(dt).name
        dtinfo = np.iinfo(dt)
        self.data[key] = np.random.randint(dtinfo.min, dtinfo.max, size=col_size, dtype=dt)

    for dt in (np.float32, np.float64):
        key = np.dtype(dt).name
        self.data[key] = np.random.rand(col_size).astype(dt)

    # var-len
    #self.data['utf_string'] = np.array([rand_utf8(np.random.randint(1, 100))
    #                               for _ in range(col_size)])
    self.data['utf_string'] = np.array([chr(ord('a') + i) * (i+1)for i in range(col_size)])
    self.data['utf_string'][3] = ''
    print(self.data['utf_string'])

    self.arrays = [pa.array(val) for val in self.data.values()]
    self.names = list(self.data.keys())

  def check(self):
    pass

d = DataFactory(10)