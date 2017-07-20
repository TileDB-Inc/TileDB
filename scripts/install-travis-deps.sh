#!/bin/sh

sudo apt-get -y install cmake lcov mpich zlib1g-dev libbz2-dev liblz4-dev libssl-dev

# Install Zstandard
cd $TRAVIS_BUILD_DIR/build
wget https://github.com/facebook/zstd/archive/v1.0.0.tar.gz
tar xf v1.0.0.tar.gz
cd zstd-1.0.0
sudo make install PREFIX='/usr'

#Install Blosc
cd $TRAVIS_BUILD_DIR/build
git clone https://github.com/Blosc/c-blosc 
cd c-blosc
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX='/usr' ..
cmake --build .
sudo cmake --build . --target install

# Install clang-format
sudo add-apt-repository 'deb http://apt.llvm.org/trusty/ llvm-toolchain-trusty main'
wget -O - http://apt.llvm.org/llvm-snapshot.gpg.key|sudo apt-key add -
sudo apt-get update -qq 
sudo apt-get install -qq -y clang-format-5.0
