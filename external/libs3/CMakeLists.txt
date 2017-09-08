# find packages
find_package(CURL REQUIRED)
include_directories(SYSTEM ${CURL_INCLUDE_DIRS})

find_package(OpenSSL REQUIRED)
include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})

find_package(LibXml2 REQUIRED)
include_directories(${LIBXML2_INCLUDE_DIR})

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/inc/")

set(LIBS3_SOURCES
  src/bucket.c   
  src/general.c            
  src/request.c           
  src/s3.c                
  src/simplexml.c
  src/bucket_metadata.c        
  src/multipart.c            
  src/request_context.c        
  src/service.c            
  src/util.c
  src/error_parser.c            
  src/object.c            
  src/response_headers_handler.c    
  src/service_access_logging.c
)

add_library(libs3 SHARED ${LIBS3_SOURCES})
target_link_libraries(libs3
  ${LIBXML2_LIBRARIES} ${CURL_LIBRARIES} ${OPENSSL_LIBRARIES})
