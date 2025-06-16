

find_package(Threads REQUIRED)
# target_link_libraries(BiSearch ${CMAKE_THREAD_LIBS_INIT})

add_executable(DFree main.cc)
target_link_libraries(DFree ${FINAL_OBJ} xdelta3 ${CMAKE_THREAD_LIBS_INIT})

