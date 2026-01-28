file(GLOB METHOD_THREAD_SRC "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(method_thread OBJECT ${METHOD_THREAD_SRC})

target_include_directories(method_thread PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)