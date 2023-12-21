FROM dokken/centos-stream-9

RUN yum -y install git g++ python3-pip curl zip unzip tar \
    && pip install cmake

COPY . .

RUN cmake . -B build -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DCMAKE_MAKE_PROGRAM=make \
    && cmake --build build \
    && cmake --install build