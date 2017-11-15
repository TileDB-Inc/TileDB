mkdir /tmp/minio-data
docker run -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=miniosecretkey -d -p 9000:9000 minio/minio server /tmp/minio-data

export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=miniosecretkey

git clone https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp
git checkout 1.2.0
mkdir build
cd build
export AWS_SDK_CPP=$(pwd)
#cmake -DCMAKE_BUILD_TYPE=Release ..
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_ONLY="s3;core;transfer;config" ..
make
sudo make install
sed -i 's/-fno-exceptions;//g' aws-cpp-sdk-core/aws-cpp-sdk-core-targets.cmake
