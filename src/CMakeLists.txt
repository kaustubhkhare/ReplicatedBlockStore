cmake_minimum_required(VERSION 3.10)

project(project3)

add_executable(client client.cc)
add_executable(health_client health_client.cc)
add_executable(server server.cpp)
add_executable(test3 test3.cpp)

find_package(OpenSSL REQUIRED)
target_link_libraries(client Threads::Threads gRPC::grpc++ p3protolib)
target_link_libraries(test3 Threads::Threads gRPC::grpc++ p3protolib)
target_link_libraries(client OpenSSL::SSL)
target_link_libraries(test3 OpenSSL::SSL)
target_link_libraries(server Threads::Threads gRPC::grpc++ p3protolib)
#target_link_libraries(serverC Threads::Threads gRPC::grpc++ p3protolib)


add_dependencies(client p3protolib)
add_dependencies(server p3protolib)
add_dependencies(test3 p3protolib)
target_link_libraries(health_client Threads::Threads gRPC::grpc++ p3protolib)

add_dependencies(client p3protolib)
add_dependencies(server p3protolib)
add_dependencies(health_client p3protolib)
add_dependencies(test3 p3protolib)

set_target_properties(client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
set_target_properties(server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
set_target_properties(test3 PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)
set_target_properties(health_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)