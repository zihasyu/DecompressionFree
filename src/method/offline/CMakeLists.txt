file(GLOB METHOD_OFFLINE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(method_offline OBJECT ${METHOD_OFFLINE_SRC})

target_include_directories(method_offline PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)