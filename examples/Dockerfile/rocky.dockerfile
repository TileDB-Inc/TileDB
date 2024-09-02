FROM rockylinux:9

RUN yum -y install git g++ python3-pip zip unzip tar perl \
    && pip install cmake

WORKDIR /opt/tiledb
COPY . .

RUN cmake . -B build -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DCMAKE_MAKE_PROGRAM=make \
      -DTILEDB_AZURE=ON -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DTILEDB_TOOLS=ON \
      -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build build \
    && cmake --build build --target install-tiledb \
    && ldconfig
