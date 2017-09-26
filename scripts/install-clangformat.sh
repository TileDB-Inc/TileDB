#!/bin/sh

# Install clang-format
sudo add-apt-repository 'deb http://apt.llvm.org/trusty/ llvm-toolchain-trusty-5.0 main'
wget -O - http://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
sudo apt-get update -qq 
sudo apt-get install -qq -y clang-format-5.0
