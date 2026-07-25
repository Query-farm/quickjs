# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(quickjs
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    EXTENSION_VERSION "2026072501"
    # Wasm: the loadable-extension emcc link (-sSIDE_MODULE=2) only includes
    # libraries named in LINKED_LIBS; target_link_libraries(qjs) is ignored for
    # it, so the in-tree QuickJS lib must be named here or its symbols are left
    # undefined (extension loads, then throws "n is not a function" on first
    # call). The generator expression resolves to the built libqjs.a path.
    LINKED_LIBS "$<TARGET_FILE:qjs>"
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
