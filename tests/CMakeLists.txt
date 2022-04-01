cmake_minimum_required(VERSION 3.10)

project(project3_test)

include_directories(project3_test .)
add_executable(test test1.cpp)
target_link_libraries(test Threads::Threads gRPC::grpc++ p3protolib)
target_link_libraries(test OpenSSL::SSL)
add_dependencies(test p3protolib)

set_target_properties(test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)

