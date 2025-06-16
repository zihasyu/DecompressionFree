find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED COMPONENTS thread system)
find_package(ZLIB REQUIRED)
include_directories(${DFree_SOURCE_DIR}/include)
link_directories(${DFree_SOURCE_DIR}/lib)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    link_directories(${Boost_LIBRARY_DIRS})
else()
    message(FATAL_ERROR "Cannot find boost library")
endif()

if(OPENSSL_FOUND)
    # include_directories(${OPENSSL_INCLUDE_DIR})
    # link_directories(${OPENSSL_LIBRARIES})
else()
    message(FATAL_ERROR "Cannot find OpenSSL library")
endif()

message(STATUS "Find OpenSSL version: ${OPENSSL_VERSION}")
if(OPENSSL_VERSION VERSION_LESS "1.1")
    message(FATAL_ERROR "OpenSSL version is less than 1.1")
endif()

set(BOOST_LIBRARY_OBJ boost_thread boost_system boost_serialization)

set(OPENSSL_LIBRARY_OBJ ssl crypto)

set(INSIDE_OBJ chunkcore datawritecore methodcore   utilcore)

set(ZLIB_LIBRARY_OBJ ${ZLIB_LIBRARIES})

set(FINAL_OBJ ${OPENSSL_LIBRARY_OBJ} ${INSIDE_OBJ} ${BOOST_LIBRARY_OBJ} ${ZLIB_LIBRARY_OBJ})

add_subdirectory(app)
add_subdirectory(method)
add_subdirectory(util)
add_subdirectory(chunking)
add_subdirectory(datawrite)