cmake_minimum_required(VERSION 3.10)

project(project3)

add_executable(client client.cc)

target_link_libraries(client 
	Threads::Threads
       	gRPC::grpc++
	p3protolib)
add_dependencies(client p3protolib)
set_target_properties(client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/)

