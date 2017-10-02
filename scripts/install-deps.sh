#!/bin/sh

if [ -n "$(command -v apt-get)" ]
then
  sudo apt-get -y install cmake lcov zlib1g-dev libbz2-dev liblz4-dev || exit
elif [ -n "$(command -v yum)" ]
then
  sudo yum -y install wget cmake zlib-devel bzip2-devel lz4-devel || exit
fi

TMPBUILD=`mktemp -d -t TILEDB_BUILD_XXXX`

# Install Zstandard
cd $TMPBUILD &&
wget https://github.com/facebook/zstd/archive/v1.0.0.tar.gz &&
tar xf v1.0.0.tar.gz &&
cd zstd-1.0.0 &&
sudo make install PREFIX='/usr'

#Install Blosc
cd $TMPBUILD &&
git clone https://github.com/Blosc/c-blosc  &&
cd c-blosc &&
mkdir build &&
cd build &&
cmake -DCMAKE_INSTALL_PREFIX='/usr' .. &&
cmake --build . &&
sudo cmake --build . --target install
