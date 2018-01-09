# TileDB

[![Travis](https://travis-ci.org/TileDB-Inc/TileDB.svg?branch=dev)](https://travis-ci.org/TileDB-Inc/TileDB)

The TileDB documentation can found at [docs.tiledb.io](https://docs.tiledb.io).

## HDFS on Windows

Note: HDFS support is a work in progress.

1. Install JDK 1.8 to `c:\jdk1.8.0_{version}`
1. Download the Hadoop 3.0 binary `tar.gz` file from https://hadoop.apache.org/releases.html
1. Extract the Hadoop tarball to `c:\hadoop`
1. Download the library binaries from https://github.com/steveloughran/winutils:
    1. Place the files `winutil.exe`, `hdfs.dll`, `hdfs.lib`, `hadoop.dll`, `hadoop.lib`, `libwinutils.lib` into `c:\hadoop\bin`.
1. Rename `hdfs.lib` to `hdfs_static.lib`
1. Now we have to generate a new `hdfs.lib` DLL import library suitable for dynamic linking. The `hdfs.lib` from the Winutils repo is the static library, not the import library. To understand what's going on here see [here](https://support.microsoft.com/en-us/help/131313/how-to-create-32-bit-import-libraries-without--objs-or-source) and [here](https://stackoverflow.com/questions/9946322/how-to-generate-an-import-library-lib-file-from-a-dll#9946390).
    1. Open cmd.exe and execute:
```
> "c:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
> dumpbin /exports /machine:x64 hdfs.dll > exports
> echo LIBRARY HDFS > hdfs.def
> echo EXPORTS >> hdfs.def
> for /f "skip=19 tokens=4" %A in (exports) do echo %A >> hdfs.def
> lib /def:hdfs.def /out:hdfs.lib /machine:x64
> del exports
```
7. Now we must configure HDFS to run local daemons.

    i. Create directories `c:\hadoop\data`, `c:\hadoop\data\namenode`, and `c:\hadoop\data\datanode`
    
    ii. Edit `c:\hadoop\etc\hadoop\core-site.xml`:
```
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

  iii. Edit `c:\hadoop\etc\hadoop\mapred-site.xml`:
```
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

   iv. Edit `c:\hadoop\etc\hadoop\hdfs-site.xml`:
```
<configuration>
  <property>
    <name>dfs.replication</name> 
    <value>1</value> 
  </property>
  <property>
    <name>dfs.namenode.name.dir</name> 
    <value>/hadoop/data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/hadoop/data/datanode</value> 
  </property>
</configuration>
```

   v. Edit `c:\hadoop\etc\hadoop\yarn-site.xml`:
```
<configuration> 
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
</configuration>

```

8. Now we can start HDFS:
```
> cd c:\Program Files\Java\jre1.8.0_31
> for %I in (.) do echo %~sI
c:\PROGRA~1\Java\JRE18~1.0_3
> set HADOOP_HOME=c:\hadoop\bin
> set JAVA_HOME=c:\PROGRA~1\Java\JRE18~1.0_3
> cd c:\hadoop\bin
> hdfs.cmd namenode -format
> ..\sbin\start-dfs.cmd
```

9. Navigate to http://localhost:9870 to verify it worked.

10. Now we can build TileDB. Open PowerShell and navigate to the TileDB root directory and execute:

```
PS> mkdir build
PS> cd build
PS> $env:JAVA_HOME = "C:\jdk1.8.0_151"
PS> $env:HADOOP_HOME = "C:\hadoop"
PS> ..\bootstrap.ps1 -Hdfs
PS> cmake --build . --target check --config Release
```
