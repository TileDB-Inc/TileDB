INSTALL
=======

* wget https://github.com/google/googletest/archive/release-1.7.0.tar.gz
* tar xf release-1.7.0.tar.gz
* cd googletest-release-1.7.0
* cmake -DBUILD_SHARED_LIBS=ON .
* make
* $ sudo cp -a include/gtest /usr/include
* $ sudo cp -a libgtest_main.so libgtest.so /usr/lib/

Check
=====

$ sudo ldconfig -v | grep gtest

Output should look like this:

* libgtest.so.0 -> libgtest.so.0.0.0
* libgtest_main.so.0 -> libgtest_main.so.0.0.0

Google Test framework is now ready to use. Just don't forget to link
your project against the library by setting -lgtest as linker flag
and optionally, if you did not write your own test mainroutine,
the explicit -lgtest_main flag.

