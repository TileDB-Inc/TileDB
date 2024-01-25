set -x

if [[ "$TILEDB_CI_OS" == "Linux" ]]; then

  echo "Linux: enabling core dumps, dumping cpuinfo, dumping lscpu"

  ulimit -c unlimited     # Enable core dumps to be captured (must be in same run block)
  ulimit -c
  sudo apt-get update
  sudo apt-get -y install gdb ninja-build git curl zip unzip tar pkg-config
  if [[ -f $(which gdb) ]]; then
    echo found $(which gdb)
  else
    echo tried to install gdb, but gdb not found!
  fi
  echo "core_pattern follows..."
  cat /proc/sys/kernel/core_pattern
  echo "...core_pattern above"
fi

if [[ "$TILEDB_CI_OS" == "macOS" ]]; then

  echo "macOS: enabling core dumps"

  sudo chown :staff /cores
  sudo chmod g+w /cores
  ulimit -c unlimited     # Enable core dumps to be captured (must be in same run block)
  ls -ld /cores
  ulimit -c
fi

if [[ "$CC" == "gcc-13" ]]; then
  sudo apt install -y gcc-13 g++-13
fi
