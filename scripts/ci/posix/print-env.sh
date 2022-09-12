echo "'uname -s' is:"
echo "uname: " $(uname)
echo "uname -m: " $(uname -m)
echo "uname -r:" $(uname -r)
echo "uname -s: " $(uname -s)
echo "uname -v: " $(uname -v)
printenv

cat /proc/cpuinfo

if [[ $(which lscpu) ]] ; then
  lscpu
fi
