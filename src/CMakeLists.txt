cmake_minimum_required(VERSION 3.10)

project(project3)

#include_directories(project3 include)
add_executable(client client.cc)
add_executable(server serverC.cpp)
#add_executable(serverB serverB.cpp)

target_link_libraries(client Threads::Threads gRPC::grpc++ p3protolib)
target_link_libraries(server Threads::Threads gRPC::grpc++ p3protolib)
#target_link_libraries(serverB Threads::Threads gRPC::grpc++ p3protolib)

add_dependencies(client p3protolib)
add_dependencies(server p3protolib)
#add_dependencies(serverB p3protolib)

set_target_properties(client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
set_target_properties(server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
#set_target_properties(serverB PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)