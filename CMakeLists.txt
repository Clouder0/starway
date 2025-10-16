cmake_minimum_required(VERSION 3.25)

project(starway VERSION 0.5.0 LANGUAGES CXX) # Replace 'my_project' with the name of your project

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_EXTENSIONS OFF) 

set(DEV_MODULE Development.Module)

find_package(Python 3.10 COMPONENTS Interpreter ${DEV_MODULE} REQUIRED)
message(STATUS "Found Python: ${Python_EXECUTABLE}")

# Detect the installed nanobind package and import it into CMake
execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)
message(STATUS "Found nanobind: ${nanobind_INCLUDE_DIRS}")

find_package(ucx REQUIRED COMPONENTS ucp uct ucs)
message(STATUS "Found OpenUCX Version: ${ucx_VERSION}")

nanobind_add_module(_bindings
    NB_STATIC
    LTO
    NOMINSIZE
    src/bindings/main.cpp
)

target_link_libraries(_bindings PRIVATE ucx::ucp ucx::uct ucx::ucs)
if (SKBUILD)
  install(TARGETS _bindings LIBRARY DESTINATION starway)
else()
  install(TARGETS _bindings LIBRARY DESTINATION src/starway)
endif()
