#Needs to be run from inside a current build directory
$env:BUILD_DIR = (Convert-Path .)
write-host "build_dir: ${env:build_dir}"
$env:PATH += ";" + $env:BUILD_DIR + "/externals/install/bin"

$capnppath = (Join-Path $env:BUILD_DIR "externals/install/bin/capnp.exe")
if (!(Test-Path $capnppath)){
  echo "Abort: Missing ${capnppath}"
  return 1
}

push-location
cd $args[1]  # location of tiledb-rest.capnp

$capnpincluderoot = (Join-Path $env:BUILD_DIR "externals/install/include")
write-host "capnpincluderoot: ${capnpincluderoot}"
write-host "path: ${env:PATH}"

convert-path .

write-host "capnppath: $capnppath"
write-host "args.count: " + $args.length
for($i = 0 ; $i -lt $args.length ; $i++) {
  write-host "i, $i, args[i] " + $args[$i]
}
if ($args.length -gt 0) {
  $outdir = $args[0]
  if (! (Test-Path $outdir) ) {
    New-Item -ItemType Directory -Path $outdir
  }
  $capnpcmd = "$capnppath compile -I $capnpincluderoot -oc++:$outdir tiledb-rest.capnp"
} else {
  $capnpcmd = "$capnppath compile -I $capnpincluderoot -oc++ tiledb-rest.capnp"
}
write-host "capnpcmd: $capnpcmd"
cmd /c "$capnpcmd"
pop-location
