FROM dokken/centos-stream-9

RUN yum -y install git g++ python3-pip curl zip unzip tar perl \
    && pip install cmake

COPY . .

RUN cmake . -B build -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DCMAKE_MAKE_PROGRAM=make \
      -DTILEDB_AZURE=ON -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DTILEDB_TOOLS=ON \
    && cmake --build build \
    && cmake --install build --prefix=/usr/local