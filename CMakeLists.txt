set(CMAKE_BUILD_TYPE "Release")
# set(CMAKE_BUILD_TYPE "Debug")
cmake_minimum_required(VERSION 3.1)
project(DFree)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_VERBOSE_MAKEFILE TRUE)
set(CMAKE_MODULE_PATH /usr/share/cmake-3.10/Modules/)

set(EXECUTABLE_OUTPUT_PATH ${DFree_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${DFree_SOURCE_DIR}/lib)

set(CMAKE_C_FLAGS_DEBUG "-O0 -Wall -g -ggdb3")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -Wall -fsanitize=address -fno-omit-frame-pointer -g -ggdb3") 
set(CMAKE_C_FLAGS_RELEASE "-O3 -Wall") 
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -Wall") 

set(CMAKE_C_COMPILER "/usr/bin/clang")
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")
set(CMAKE_AR "/usr/bin/llvm-ar")
set(CMAKE_LINKER "/usr/bin/llvm-ld")
set(CMAKE_NM "/usr/bin/llvm-nm")
set(CMAKE_OBJDUMP "/usr/bin/llvm-objdump")
set(CMAKE_RANLIB "/usr/bin/llvm-ranlib")

add_subdirectory(src)