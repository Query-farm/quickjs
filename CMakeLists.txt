cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME quickjs)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_subdirectory(quickjs)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/quickjs_extension.cpp src/query_farm_telemetry.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link against quickjs
target_link_libraries(${EXTENSION_NAME} qjs)
target_link_libraries(${LOADABLE_EXTENSION_NAME} qjs)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

install(
    TARGETS qjs
    EXPORT "${DUCKDB_EXPORT_SET}"
)

install(
  EXPORT "${DUCKDB_EXPORT_SET}"
  NAMESPACE "duckdb::"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/DuckDB"
  FILE "DuckDB-extension-targets.cmake"
)
