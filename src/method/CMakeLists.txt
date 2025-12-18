aux_source_directory(. METHOD_SRC)
add_subdirectory(offline)
# add_library(methodcore ${METHOD_SRC})
add_library(methodcore ${METHOD_SRC} $<TARGET_OBJECTS:method_offline>)