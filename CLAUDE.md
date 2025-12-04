# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DuckDB QuickJS Extension - embeds the QuickJS-NG JavaScript engine into DuckDB, allowing JavaScript execution within SQL queries.

## Build Commands

```sh
# Build release (default)
GEN=ninja make

# Build debug
GEN=ninja make debug

# Run tests
make test

# Format code
make format

# Run clang-tidy
make tidy-check
```

It is best to test with a debug build since it tests more assertions.

All extension functions should be documented inside of DuckDB with CreateScalarFunctionInfo or CreateAggregateFunctionInfo or the appropriate type for the function.  This documentation of the function should include examples, parameter types and parameter names.  The function should be categorized.

When making changes the version should always be updated to the current date plus an ordinal counter in the form of YYYYMMDDCC.


## Build Outputs

- `./build/release/duckdb` - DuckDB shell with extension auto-loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/quickjs/quickjs.duckdb_extension` - Loadable extension binary

## Testing

Tests are in `test/sql/quickjs.test` using DuckDB's SQL test format. Run a specific test pattern:
```sh
./build/release/test/unittest "test/sql/quickjs.test"
```

## Architecture

This is a DuckDB extension built using the extension template pattern:

- **Main source**: `src/quickjs_extension.cpp` - Contains all extension logic
- **Header**: `src/include/quickjs_extension.hpp` - Extension class declaration
- **QuickJS submodule**: `quickjs/` - The QuickJS-NG engine
- **DuckDB submodule**: `duckdb/` - DuckDB source for building

### Extension Functions

Three SQL functions are registered:

1. **`quickjs(code)` (scalar)** - Executes JS, returns result as VARCHAR
2. **`quickjs_eval(function, ...args)` (scalar)** - Executes JS function with args, returns JSON
3. **`quickjs(code, ...args)` (table)** - Executes JS returning array, each element becomes a row

### Key Implementation Details

- Each function call creates a fresh `JSRuntime`/`JSContext` for complete state isolation
- DuckDB types are converted to JS values via `DuckDBValueToJSValue()`
- Table function uses bind/init/execute pattern with `QuickJSTableBindData` and `QuickJSTableGlobalState`
- Supports both legacy (`DatabaseInstance&`) and new (`ExtensionLoader&`) DuckDB extension APIs via `DUCKDB_CPP_EXTENSION_ENTRY` preprocessor flag

### Table Function Parameter Convention

- `arg0`, `arg1`, etc. - Raw parameters
- `parsed_arg0` - First parameter parsed as JSON (for array/object inputs)

## Submodule Management

```sh
# Initialize submodules
git submodule update --init --recursive

# Update submodules to latest
make update
```

## Requirements

- DuckDB 1.3.1+
- C++11 compatible compiler
- CMake 3.16+
