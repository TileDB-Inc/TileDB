if [[ "$TILEDB_CI_OS" == "Linux" ]]; then

  # following contains answers with various possibilities for locating core files on various systems
  # https://stackoverflow.com/questions/2065912/core-dumped-but-core-file-is-not-in-the-current-directory
  pwd
  if [[ ! $(find /var/lib/apport/coredump -name 'core.*') ]]; then
    # if we don't find the core files where/as expected, then
    # the artifacts won't get uploaded (haven't figured out how to
    # dynamically specify location to upload action), but we
    # can still present stack traces for the ones we find.
    echo "core files not found where expected!"
    if [[ $(find . -name 'core.*') ]]; then
      echo "core files found at/under $(pwd)"
      corefiles=$(find . -name 'core.*')
    elif [[ $(find / -name 'core.*') ]]; then
      echo "core files found!"
      corefiles=$(find / -name 'core.*')
    else
      echo "core files expected but not found!"
      corefiles=
    fi
  else
    corefiles=$(find /var/lib/apport/coredump -name 'core.*')
  fi
  ulimit -c
  echo "ls -l /var/lib/apport/coredump"
  ls -l /var/lib/apport/coredump
  for f in $corefiles;
    do
      echo "stack trace for $f"
      if [[ -f $(which gdb) ]]; then
        dbgr=$(which gdb)
        sudo $dbgr -q --core "$f" -ex "thread apply all bt" -ex "info registers" -ex "disassemble" -ex "quit"
      #fi
      # lldb ref'd here located by doing trial in runner with find / -name "*lldb*" and sifting through the results.
      elif [[ -f /usr/lib/llvm-11/bin/lldb ]]; then
        dbgr="/usr/lib/llvm-11/bin/lldb"
        sudo "$dbgr" -c "$f" --batch -o 'bt all' -o 'image list' -o 're r -a' -o 'di -F intel -f -m' -o 'quit'
      else
        echo "debugger not found in previously seen location!"
        exit 1
      fi
  done;
fi

if [[ "$TILEDB_CI_OS" == "macOS" ]]; then
  nfiles=$(ls /cores | wc -l)
  if [[ $nfiles -eq 0 ]]; then
    echo "no core files found"
    exit 0
  fi
  ls -la /cores
  for f in $(find /cores -name 'core.*');
    do
      echo "stack trace for $f"
      lldb -c $f --batch -o 'bt all' -o 'image list' -o 're r -a' -o 'di -F intel -f -m' -o 'quit'
    done;
fi
