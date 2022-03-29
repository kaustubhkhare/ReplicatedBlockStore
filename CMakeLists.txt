cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)

# CMakeLists.txt
set (VCPKG_HOME ~/installs/vcpkg/) #IMPORTANT: change this to where your vcpkg exists.
set (CMAKE_TOOLCHAIN_FILE "${VCPKG_HOME}/scripts/buildsystems/vcpkg.cmake")
set (CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE Release)

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

project(project3)

find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

include_directories(project3 include)
add_subdirectory(external)
add_subdirectory(src)